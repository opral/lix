use crate::json_store::compression::{compress_json_payload, decode_json_zstd_payload};
use crate::json_store::encoded::{EncodedJson, JsonCodec};
use crate::json_store::types::{JsonReadScopeRef, JsonRef};
use crate::storage::{KvGetGroup, KvGetRequest, StorageReader};
use crate::LixError;
use std::borrow::Cow;
use std::collections::BTreeMap;

pub(crate) const JSON_NAMESPACE: &str = "json_store.json";
pub(crate) const JSON_PACK_NAMESPACE: &str = "json_store.pack";
const STORED_JSON_MAGIC: &[u8] = b"lix-json:v1";
const STORED_JSON_HEADER_LEN: usize = STORED_JSON_MAGIC.len() + 1 + 8;
const STORED_JSON_PACK_MAGIC: &[u8] = b"lix-json-pack:v1";
const STORED_JSON_PACK_ENTRY_HEADER_LEN: usize = 32 + 1 + 8 + 8 + 8;
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

pub(crate) fn json_ref_for_content(bytes: &[u8]) -> JsonRef {
    JsonRef::for_content(bytes)
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

pub(crate) fn encode_direct_json_payload(encoded_json: &EncodedJson<'_>) -> Vec<u8> {
    encode_stored_json_payload(encoded_json)
}

pub(crate) fn pack_key(commit_id: &str, pack_id: u32) -> Vec<u8> {
    let commit_id = commit_id.as_bytes();
    let mut key = Vec::with_capacity(4 + commit_id.len() + 4);
    key.extend_from_slice(&(commit_id.len() as u32).to_be_bytes());
    key.extend_from_slice(commit_id);
    key.extend_from_slice(&pack_id.to_be_bytes());
    key
}

pub(crate) fn encode_json_pack(entries: &[&EncodedJson<'_>]) -> Result<Vec<u8>, LixError> {
    let mut directory_len =
        STORED_JSON_PACK_MAGIC.len() + 4 + entries.len() * STORED_JSON_PACK_ENTRY_HEADER_LEN;
    let payload_len = entries
        .iter()
        .map(|entry| entry.data.as_ref().len())
        .sum::<usize>();
    let mut out = Vec::with_capacity(directory_len + payload_len);
    out.extend_from_slice(STORED_JSON_PACK_MAGIC);
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());

    let mut offset = 0_u64;
    for entry in entries {
        let data = entry.data.as_ref();
        out.extend_from_slice(entry.json_ref.as_hash_bytes());
        out.push(json_codec_byte(entry.codec));
        out.extend_from_slice(&(entry.uncompressed_len as u64).to_be_bytes());
        out.extend_from_slice(&offset.to_be_bytes());
        out.extend_from_slice(&(data.len() as u64).to_be_bytes());
        offset = offset.checked_add(data.len() as u64).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "json_store pack payload offset overflow",
            )
        })?;
    }
    for entry in entries {
        out.extend_from_slice(entry.data.as_ref());
    }
    directory_len = out.len() - payload_len;
    debug_assert_eq!(
        directory_len,
        STORED_JSON_PACK_MAGIC.len() + 4 + entries.len() * STORED_JSON_PACK_ENTRY_HEADER_LEN
    );
    Ok(out)
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

async fn load_json_bytes_direct(
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
    let _ = store;
    decode_json_payload(json_ref, stored_payload).map(Some)
}

pub(crate) async fn load_json_bytes_many_in_scope(
    store: &mut impl StorageReader,
    json_refs: &[JsonRef],
    scope: JsonReadScopeRef<'_>,
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    if json_refs.is_empty() {
        return Ok(Vec::new());
    }

    let mut unique_keys = Vec::new();
    let mut unique_refs = Vec::new();
    let mut key_indexes = BTreeMap::<Vec<u8>, usize>::new();
    let mut requested_indexes = Vec::with_capacity(json_refs.len());
    let mut has_duplicate_refs = false;
    for json_ref in json_refs {
        let key = json_ref.as_hash_bytes().to_vec();
        let index = match key_indexes.get(&key) {
            Some(index) => {
                has_duplicate_refs = true;
                *index
            }
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

    let mut unique_values = match scope {
        JsonReadScopeRef::OutOfBand => vec![None; unique_refs.len()],
        JsonReadScopeRef::CommitPacks {
            commit_id,
            pack_ids,
        } => load_from_packs(store, &unique_refs, commit_id, pack_ids).await?,
    };

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

    let result = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: JSON_NAMESPACE.to_string(),
                keys: missing
                    .iter()
                    .map(|&index| unique_keys[index].clone())
                    .collect(),
            }],
        })
        .await?;
    let group = result.groups.into_iter().next().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "json_store batch load returned no result group",
        )
    })?;
    if group.len() != missing.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "json_store batch load returned {} values for {} requested refs",
                group.len(),
                missing.len()
            ),
        ));
    }

    for (index, stored_bytes) in group.values_iter().enumerate() {
        let unique_index = missing[index];
        let Some(stored_bytes) = stored_bytes else {
            continue;
        };
        let stored_payload = decode_stored_json_payload(stored_bytes)?;
        let _ = store;
        unique_values[unique_index] = Some(decode_json_payload(
            &unique_refs[unique_index],
            stored_payload,
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

async fn load_from_packs(
    store: &mut impl StorageReader,
    unique_refs: &[JsonRef],
    commit_id: &str,
    pack_ids: &[u32],
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    let mut values = vec![None; unique_refs.len()];
    if pack_ids.is_empty() || unique_refs.is_empty() {
        return Ok(values);
    }
    let wanted = unique_refs
        .iter()
        .enumerate()
        .map(|(index, json_ref)| (json_ref.as_hash_bytes().to_vec(), index))
        .collect::<BTreeMap<_, _>>();
    let keys = pack_ids
        .iter()
        .map(|&pack_id| pack_key(commit_id, pack_id))
        .collect::<Vec<_>>();
    let result = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: JSON_PACK_NAMESPACE.to_string(),
                keys,
            }],
        })
        .await?;
    let group = result.groups.into_iter().next().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "json_store pack load returned no result group",
        )
    })?;
    for stored_pack in group.values_iter().flatten() {
        for (json_ref, value) in decode_json_pack(stored_pack)? {
            if let Some(&index) = wanted.get(json_ref.as_hash_bytes()) {
                values[index] = Some(value);
            }
        }
    }
    Ok(values)
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

fn decode_json_pack(bytes: &[u8]) -> Result<Vec<(JsonRef, Vec<u8>)>, LixError> {
    if bytes.len() < STORED_JSON_PACK_MAGIC.len() + 4 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "stored JSON pack is truncated",
        ));
    }
    if &bytes[..STORED_JSON_PACK_MAGIC.len()] != STORED_JSON_PACK_MAGIC {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "stored JSON pack has invalid header",
        ));
    }
    let count_start = STORED_JSON_PACK_MAGIC.len();
    let count_end = count_start + 4;
    let count = u32::from_be_bytes(
        bytes[count_start..count_end]
            .try_into()
            .expect("json pack count header is fixed size"),
    ) as usize;
    let directory_start = count_end;
    let directory_len = count
        .checked_mul(STORED_JSON_PACK_ENTRY_HEADER_LEN)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "json pack directory overflow",
            )
        })?;
    let payload_start = directory_start.checked_add(directory_len).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "json pack payload offset overflow",
        )
    })?;
    if bytes.len() < payload_start {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "stored JSON pack directory is truncated",
        ));
    }

    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let mut cursor = directory_start + index * STORED_JSON_PACK_ENTRY_HEADER_LEN;
        let hash: [u8; 32] = bytes[cursor..cursor + 32]
            .try_into()
            .expect("json pack hash header is fixed size");
        cursor += 32;
        let codec = read_json_codec(bytes[cursor])?;
        cursor += 1;
        let uncompressed_len = u64::from_be_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .expect("json pack uncompressed length is fixed size"),
        ) as usize;
        cursor += 8;
        let offset = u64::from_be_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .expect("json pack payload offset is fixed size"),
        ) as usize;
        cursor += 8;
        let len = u64::from_be_bytes(
            bytes[cursor..cursor + 8]
                .try_into()
                .expect("json pack payload length is fixed size"),
        ) as usize;
        let data_start = payload_start.checked_add(offset).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "json pack entry offset overflow",
            )
        })?;
        let data_end = data_start.checked_add(len).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "json pack entry length overflow",
            )
        })?;
        if data_end > bytes.len() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "stored JSON pack entry payload is truncated",
            ));
        }
        let json_ref = JsonRef::from_hash_bytes(hash);
        let payload = StoredJsonPayload {
            codec,
            uncompressed_len,
            data: &bytes[data_start..data_end],
        };
        let value = decode_json_payload(&json_ref, payload)?;
        out.push((json_ref, value));
    }
    Ok(out)
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
            load_json_bytes_direct(&mut store, &encoded.json_ref)
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
        let values = load_json_bytes_many_in_scope(
            &mut store,
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
}
