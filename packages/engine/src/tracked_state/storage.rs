use crate::storage::KvScanRange;
use crate::storage::{
    KvGetGroup, KvGetProjection, KvGetRequest, KvPut, KvRowBatch, KvScanProjection, KvScanRequest,
    KvWriteBatch, KvWriteGroup, StorageReader, StorageWriter,
};
use crate::tracked_state::codec::PendingChunkWrite;
use crate::tracked_state::tree_types::{
    SnapshotCodec, SnapshotRef, TrackedStateRootId, TRACKED_STATE_HASH_BYTES,
};
use crate::LixError;
use serde_json::Value as JsonValue;
use std::borrow::Cow;

const ZSTD_MIN_SNAPSHOT_BYTES: usize = 16 * 1024;
const MIN_ZSTD_SAVINGS_BYTES: usize = 128;
const JSON_CHUNK_MIN_SNAPSHOT_BYTES: usize = 16 * 1024;
const JSON_CHUNK_TARGET_BYTES: usize = 4 * 1024;

pub(crate) const TRACKED_STATE_CHUNK_NAMESPACE: &str = "tracked_state.tree.chunk";
pub(crate) const TRACKED_STATE_ROOT_NAMESPACE: &str = "tracked_state.tree.root";
pub(crate) const TRACKED_STATE_BY_FILE_ROOT_NAMESPACE: &str = "tracked_state.tree.root.by_file";
pub(crate) const TRACKED_STATE_SNAPSHOT_NAMESPACE: &str = "tracked_state.snapshot";
pub(crate) const TRACKED_STATE_JSON_SNAPSHOT_CHUNK_NAMESPACE: &str =
    "tracked_state.snapshot.json_chunk";

async fn get_one(
    store: &mut (impl StorageReader + ?Sized),
    namespace: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    Ok(store
        .get_kv_many(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: namespace.to_string(),
                keys: vec![key],
            }],
            projection: KvGetProjection::Values,
        })
        .await?
        .groups
        .into_iter()
        .next()
        .map(|mut group| group.pop_value())
        .transpose()?
        .flatten())
}

async fn put_one(
    writer: &mut (impl StorageWriter + ?Sized),
    namespace: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), LixError> {
    writer
        .write_kv_batch(KvWriteBatch {
            groups: vec![KvWriteGroup {
                namespace: namespace.to_string(),
                puts: vec![KvPut { key, value }],
                deletes: Vec::new(),
            }],
        })
        .await?;
    Ok(())
}

async fn scan_all(
    store: &mut (impl StorageReader + ?Sized),
    namespace: &str,
    range: KvScanRange,
) -> Result<KvRowBatch, LixError> {
    Ok(store
        .scan_kv(KvScanRequest {
            namespace: namespace.to_string(),
            range,
            after: None,
            limit: usize::MAX,
            projection: KvScanProjection::KeysAndValues,
        })
        .await?
        .into_rows())
}

pub(crate) async fn load_root(
    store: &mut (impl StorageReader + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
    )
    .await?
    else {
        return Ok(None);
    };
    TrackedStateRootId::from_slice(&bytes).map(Some)
}

pub(crate) async fn store_root(
    writer: &mut impl StorageWriter,
    commit_id: &str,
    root_id: &TrackedStateRootId,
) -> Result<(), LixError> {
    put_one(
        writer,
        TRACKED_STATE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
        root_id.as_bytes().to_vec(),
    )
    .await
}

pub(crate) async fn load_by_file_root(
    store: &mut (impl StorageReader + ?Sized),
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_BY_FILE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
    )
    .await?
    else {
        return Ok(None);
    };
    TrackedStateRootId::from_slice(&bytes).map(Some)
}

pub(crate) async fn store_by_file_root(
    writer: &mut impl StorageWriter,
    commit_id: &str,
    root_id: &TrackedStateRootId,
) -> Result<(), LixError> {
    put_one(
        writer,
        TRACKED_STATE_BY_FILE_ROOT_NAMESPACE,
        commit_id.as_bytes().to_vec(),
        root_id.as_bytes().to_vec(),
    )
    .await
}

/// Deletes the root pointer for a commit.
///
/// Chunks remain content-addressed facts. Removing the root only makes that
/// commit's tracked-state projection unavailable until it is rebuilt from the
/// changelog.
#[cfg(test)]
pub(crate) async fn delete_root(
    writer: &mut (impl StorageWriter + ?Sized),
    commit_id: &str,
) -> Result<(), LixError> {
    writer
        .write_kv_batch(KvWriteBatch {
            groups: vec![
                KvWriteGroup {
                    namespace: TRACKED_STATE_ROOT_NAMESPACE.to_string(),
                    puts: Vec::new(),
                    deletes: vec![commit_id.as_bytes().to_vec()],
                },
                KvWriteGroup {
                    namespace: TRACKED_STATE_BY_FILE_ROOT_NAMESPACE.to_string(),
                    puts: Vec::new(),
                    deletes: vec![commit_id.as_bytes().to_vec()],
                },
            ],
        })
        .await?;
    Ok(())
}

pub(crate) async fn read_chunk(
    store: &mut impl StorageReader,
    hash: &[u8; TRACKED_STATE_HASH_BYTES],
) -> Result<Option<Vec<u8>>, LixError> {
    get_one(store, TRACKED_STATE_CHUNK_NAMESPACE, hash.to_vec()).await
}

pub(crate) async fn write_chunks(
    writer: &mut impl StorageWriter,
    chunks: &[PendingChunkWrite],
) -> Result<(), LixError> {
    for chunk in chunks {
        put_one(
            writer,
            TRACKED_STATE_CHUNK_NAMESPACE,
            chunk.hash.to_vec(),
            chunk.data.clone(),
        )
        .await?;
    }
    Ok(())
}

pub(crate) struct EncodedSnapshot<'a> {
    pub(crate) snapshot_ref: SnapshotRef,
    pub(crate) data: Cow<'a, [u8]>,
    pub(crate) json_chunks: Vec<JsonChunkWrite>,
}

pub(crate) struct JsonChunkWrite {
    hash_hex: String,
    data: String,
}

pub(crate) fn raw_snapshot_ref_for_content(snapshot_content: &str) -> SnapshotRef {
    let hash_hex = blake3::hash(snapshot_content.as_bytes())
        .to_hex()
        .to_string();
    SnapshotRef {
        codec: SnapshotCodec::Raw,
        hash_hex,
        uncompressed_len: snapshot_content.len() as u64,
    }
}

#[cfg(test)]
pub(crate) fn encode_snapshot_content(
    snapshot_content: &str,
) -> Result<EncodedSnapshot<'_>, LixError> {
    let raw_ref = raw_snapshot_ref_for_content(snapshot_content);
    encode_snapshot_content_with_ref(snapshot_content, raw_ref)
}

pub(crate) fn encode_snapshot_content_with_ref(
    snapshot_content: &str,
    raw_ref: SnapshotRef,
) -> Result<EncodedSnapshot<'_>, LixError> {
    debug_assert_eq!(raw_ref.codec, SnapshotCodec::Raw);
    debug_assert_eq!(raw_ref.uncompressed_len, snapshot_content.len() as u64);
    let raw_data = snapshot_content.as_bytes();

    if raw_data.len() >= JSON_CHUNK_MIN_SNAPSHOT_BYTES {
        if let Some(json_snapshot) = encode_json_chunked_snapshot(snapshot_content)? {
            return Ok(json_snapshot);
        }
    }

    if raw_data.len() >= ZSTD_MIN_SNAPSHOT_BYTES {
        let compressed = compress_snapshot_payload(raw_data)?;
        if raw_data.len().saturating_sub(compressed.len()) >= MIN_ZSTD_SAVINGS_BYTES {
            return Ok(EncodedSnapshot {
                snapshot_ref: SnapshotRef {
                    codec: SnapshotCodec::Zstd,
                    ..raw_ref
                },
                data: Cow::Owned(compressed),
                json_chunks: Vec::new(),
            });
        }
    }

    Ok(EncodedSnapshot {
        snapshot_ref: raw_ref,
        data: Cow::Borrowed(raw_data),
        json_chunks: Vec::new(),
    })
}

pub(crate) async fn store_encoded_snapshot(
    writer: &mut impl StorageWriter,
    encoded_snapshot: &EncodedSnapshot<'_>,
) -> Result<(), LixError> {
    for chunk in &encoded_snapshot.json_chunks {
        put_one(
            writer,
            TRACKED_STATE_JSON_SNAPSHOT_CHUNK_NAMESPACE,
            chunk.hash_hex.as_bytes().to_vec(),
            chunk.data.as_bytes().to_vec(),
        )
        .await?;
    }
    store_snapshot_ref(
        writer,
        &encoded_snapshot.snapshot_ref,
        encoded_snapshot.data.as_ref(),
    )
    .await
}

pub(crate) async fn store_snapshot_ref(
    writer: &mut impl StorageWriter,
    snapshot_ref: &SnapshotRef,
    snapshot_data: &[u8],
) -> Result<(), LixError> {
    put_one(
        writer,
        TRACKED_STATE_SNAPSHOT_NAMESPACE,
        snapshot_ref.hash_hex.as_bytes().to_vec(),
        snapshot_data.to_vec(),
    )
    .await
}

pub(crate) async fn load_snapshot(
    store: &mut impl StorageReader,
    snapshot_ref: &SnapshotRef,
) -> Result<Option<String>, LixError> {
    let Some(bytes) = get_one(
        store,
        TRACKED_STATE_SNAPSHOT_NAMESPACE,
        snapshot_ref.hash_hex.as_bytes().to_vec(),
    )
    .await?
    else {
        return Ok(None);
    };
    let data = decode_snapshot_payload(store, snapshot_ref, &bytes).await?;
    String::from_utf8(data).map(Some).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state snapshot content is invalid UTF-8: {error}"),
        )
    })
}

async fn decode_snapshot_payload(
    store: &mut impl StorageReader,
    snapshot_ref: &SnapshotRef,
    bytes: &[u8],
) -> Result<Vec<u8>, LixError> {
    let data = match snapshot_ref.codec {
        SnapshotCodec::Raw => Ok(bytes.to_vec()),
        SnapshotCodec::Zstd => decode_snapshot_zstd_payload(bytes, snapshot_ref),
        SnapshotCodec::JsonChunks => decode_json_chunked_snapshot(store, snapshot_ref, bytes).await,
    }?;
    if data.len() != snapshot_ref.uncompressed_len as usize {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "tracked-state snapshot ref '{}' decoded to {} bytes, expected {}",
                snapshot_ref.hash_hex,
                data.len(),
                snapshot_ref.uncompressed_len
            ),
        ));
    }
    let actual_hash = blake3::hash(&data).to_hex().to_string();
    if actual_hash != snapshot_ref.hash_hex {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "tracked-state snapshot ref '{}' hash mismatch",
                snapshot_ref.hash_hex
            ),
        ));
    }
    Ok(data)
}

#[derive(Debug)]
struct JsonChunkManifest {
    chunks: Vec<JsonChunkRef>,
}

#[derive(Debug)]
struct JsonChunkRef {
    hash_hex: String,
    len: usize,
}

fn encode_json_chunked_snapshot(
    snapshot_content: &str,
) -> Result<Option<EncodedSnapshot<'static>>, LixError> {
    let json = match serde_json::from_str::<JsonValue>(snapshot_content) {
        Ok(json) => json,
        Err(_) => return Ok(None),
    };
    let canonical = serde_json::to_string(&json).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state JSON snapshot serialization failed: {error}"),
        )
    })?;
    let Some(chunks) = json_snapshot_chunks(&json)? else {
        return Ok(None);
    };
    if chunks.len() < 2 {
        return Ok(None);
    }
    let manifest = encode_json_chunk_manifest(&chunks);
    let json_chunks = chunks
        .into_iter()
        .map(|data| JsonChunkWrite {
            hash_hex: blake3::hash(data.as_bytes()).to_hex().to_string(),
            data,
        })
        .collect();
    let hash_hex = blake3::hash(canonical.as_bytes()).to_hex().to_string();
    Ok(Some(EncodedSnapshot {
        snapshot_ref: SnapshotRef {
            codec: SnapshotCodec::JsonChunks,
            hash_hex,
            uncompressed_len: canonical.len() as u64,
        },
        data: Cow::Owned(manifest.into_bytes()),
        json_chunks,
    }))
}

fn json_snapshot_chunks(json: &JsonValue) -> Result<Option<Vec<String>>, LixError> {
    match json {
        JsonValue::Array(items) => json_array_chunks(items),
        JsonValue::Object(fields) => json_object_chunks(fields),
        _ => Ok(None),
    }
}

fn json_array_chunks(items: &[JsonValue]) -> Result<Option<Vec<String>>, LixError> {
    let mut chunks = Vec::new();
    let mut current = String::from("[");
    let mut first_in_chunk = true;
    for item in items {
        let item_json = serde_json::to_string(item).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("tracked-state JSON array chunk serialization failed: {error}"),
            )
        })?;
        let separator_len = usize::from(!first_in_chunk);
        if !first_in_chunk
            && current.len() + separator_len + item_json.len() + 1 > JSON_CHUNK_TARGET_BYTES
        {
            current.push(']');
            chunks.push(current);
            current = String::from("[");
            first_in_chunk = true;
        }
        if !first_in_chunk {
            current.push(',');
        }
        current.push_str(&item_json);
        first_in_chunk = false;
    }
    current.push(']');
    chunks.push(current);
    Ok(Some(chunks))
}

fn json_object_chunks(
    fields: &serde_json::Map<String, JsonValue>,
) -> Result<Option<Vec<String>>, LixError> {
    let mut chunks = Vec::new();
    let mut current = String::from("{");
    let mut first_in_chunk = true;
    for (key, value) in fields {
        let key_json = serde_json::to_string(key).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("tracked-state JSON object key serialization failed: {error}"),
            )
        })?;
        let value_json = serde_json::to_string(value).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("tracked-state JSON object value serialization failed: {error}"),
            )
        })?;
        let entry_len = key_json.len() + 1 + value_json.len();
        let separator_len = usize::from(!first_in_chunk);
        if !first_in_chunk
            && current.len() + separator_len + entry_len + 1 > JSON_CHUNK_TARGET_BYTES
        {
            current.push('}');
            chunks.push(current);
            current = String::from("{");
            first_in_chunk = true;
        }
        if !first_in_chunk {
            current.push(',');
        }
        current.push_str(&key_json);
        current.push(':');
        current.push_str(&value_json);
        first_in_chunk = false;
    }
    current.push('}');
    chunks.push(current);
    Ok(Some(chunks))
}

fn encode_json_chunk_manifest(chunks: &[String]) -> String {
    let mut manifest = format!("json_chunks:v1:{}:", chunks.len());
    for chunk in chunks {
        let hash_hex = blake3::hash(chunk.as_bytes()).to_hex().to_string();
        manifest.push_str(&hash_hex);
        manifest.push(':');
        manifest.push_str(&chunk.len().to_string());
        manifest.push(':');
    }
    manifest
}

fn decode_json_chunk_manifest(bytes: &[u8]) -> Result<JsonChunkManifest, LixError> {
    let manifest = std::str::from_utf8(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state JSON snapshot manifest is invalid UTF-8: {error}"),
        )
    })?;
    let mut cursor = 0usize;
    let Some(header) = read_manifest_part(manifest, &mut cursor) else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state JSON snapshot manifest is empty",
        ));
    };
    let Some(version) = read_manifest_part(manifest, &mut cursor) else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state JSON snapshot manifest is missing version",
        ));
    };
    if header != "json_chunks" || version != "v1" {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state JSON snapshot manifest has invalid header",
        ));
    }
    let chunk_count = read_manifest_part(manifest, &mut cursor)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state JSON snapshot manifest is missing chunk count",
            )
        })?
        .parse::<usize>()
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("tracked-state JSON snapshot manifest has invalid chunk count: {error}"),
            )
        })?;
    let mut chunks = Vec::with_capacity(chunk_count);
    for _ in 0..chunk_count {
        let hash_hex = read_manifest_part(manifest, &mut cursor)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "tracked-state JSON snapshot manifest is missing chunk hash",
                )
            })?
            .to_string();
        let len = read_manifest_part(manifest, &mut cursor)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "tracked-state JSON snapshot manifest is missing chunk length",
                )
            })?
            .parse::<usize>()
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked-state JSON snapshot manifest has invalid chunk length: {error}"
                    ),
                )
            })?;
        chunks.push(JsonChunkRef { hash_hex, len });
    }
    Ok(JsonChunkManifest { chunks })
}

fn read_manifest_part<'a>(manifest: &'a str, cursor: &mut usize) -> Option<&'a str> {
    let remaining = manifest.get(*cursor..)?;
    let delimiter = remaining.find(':')?;
    let part = &remaining[..delimiter];
    *cursor += delimiter + 1;
    Some(part)
}

async fn decode_json_chunked_snapshot(
    store: &mut impl StorageReader,
    snapshot_ref: &SnapshotRef,
    manifest_bytes: &[u8],
) -> Result<Vec<u8>, LixError> {
    let manifest = decode_json_chunk_manifest(manifest_bytes)?;
    let mut out = String::new();
    let mut close_delimiter = None;
    for (index, chunk_ref) in manifest.chunks.iter().enumerate() {
        let Some(chunk_bytes) = get_one(
            store,
            TRACKED_STATE_JSON_SNAPSHOT_CHUNK_NAMESPACE,
            chunk_ref.hash_hex.as_bytes().to_vec(),
        )
        .await?
        else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "tracked-state JSON snapshot chunk '{}' is missing",
                    chunk_ref.hash_hex
                ),
            ));
        };
        let chunk = String::from_utf8(chunk_bytes).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("tracked-state JSON snapshot chunk is invalid UTF-8: {error}"),
            )
        })?;
        if chunk.len() != chunk_ref.len {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "tracked-state JSON snapshot chunk '{}' decoded to {} bytes, expected {}",
                    chunk_ref.hash_hex,
                    chunk.len(),
                    chunk_ref.len
                ),
            ));
        }
        let actual_hash = blake3::hash(chunk.as_bytes()).to_hex().to_string();
        if actual_hash != chunk_ref.hash_hex {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "tracked-state JSON snapshot chunk '{}' hash mismatch",
                    chunk_ref.hash_hex
                ),
            ));
        }
        close_delimiter = Some(append_json_chunk(&mut out, &chunk, index == 0)?);
    }
    if let Some(close) = close_delimiter {
        out.push(close);
    }
    if blake3::hash(out.as_bytes()).to_hex().to_string() != snapshot_ref.hash_hex {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "tracked-state JSON snapshot ref '{}' hash mismatch",
                snapshot_ref.hash_hex
            ),
        ));
    }
    Ok(out.into_bytes())
}

fn append_json_chunk(out: &mut String, chunk: &str, first: bool) -> Result<char, LixError> {
    let bytes = chunk.as_bytes();
    let Some((&open, body_with_close)) = bytes.split_first() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state JSON snapshot chunk is empty",
        ));
    };
    let Some((&close, body)) = body_with_close.split_last() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state JSON snapshot chunk is truncated",
        ));
    };
    let expected_close = match open {
        b'[' => b']',
        b'{' => b'}',
        _ => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state JSON snapshot chunk has invalid opening delimiter",
            ))
        }
    };
    if close != expected_close {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state JSON snapshot chunk delimiters do not match",
        ));
    }
    if out.is_empty() {
        out.push(open as char);
    } else if first {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state JSON snapshot append state is invalid",
        ));
    } else if !body.is_empty() {
        out.push(',');
    }
    out.push_str(std::str::from_utf8(body).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("tracked-state JSON snapshot chunk body is invalid UTF-8: {error}"),
        )
    })?);
    Ok(close as char)
}

#[cfg(not(target_arch = "wasm32"))]
fn compress_snapshot_payload(snapshot_data: &[u8]) -> Result<Vec<u8>, LixError> {
    zstd::bulk::compress(snapshot_data, 1).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("tracked-state snapshot compression failed: {error}"),
        hint: None,
        details: None,
    })
}

#[cfg(target_arch = "wasm32")]
fn compress_snapshot_payload(snapshot_data: &[u8]) -> Result<Vec<u8>, LixError> {
    Ok(ruzstd::encoding::compress_to_vec(
        snapshot_data,
        ruzstd::encoding::CompressionLevel::Fastest,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_snapshot_zstd_payload(
    compressed_payload: &[u8],
    snapshot_ref: &SnapshotRef,
) -> Result<Vec<u8>, LixError> {
    zstd::bulk::decompress(compressed_payload, snapshot_ref.uncompressed_len as usize).map_err(
        |error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!(
                "tracked-state snapshot decompression failed for ref '{}': {error}",
                snapshot_ref.hash_hex
            ),
            hint: None,
            details: None,
        },
    )
}

#[cfg(target_arch = "wasm32")]
fn decode_snapshot_zstd_payload(
    compressed_payload: &[u8],
    _snapshot_ref: &SnapshotRef,
) -> Result<Vec<u8>, LixError> {
    use std::io::Read as _;

    let mut decoder =
        ruzstd::decoding::StreamingDecoder::new(compressed_payload).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: format!("tracked-state snapshot decompression failed: {error}"),
            hint: None,
            details: None,
        })?;

    let mut output = Vec::new();
    decoder.read_to_end(&mut output).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!("tracked-state snapshot decompression failed: {error}"),
        hint: None,
        details: None,
    })?;
    Ok(output)
}

#[allow(dead_code)]
pub(crate) async fn scan_roots(
    store: &mut impl StorageReader,
) -> Result<Vec<(String, TrackedStateRootId)>, LixError> {
    let pairs = scan_all(
        store,
        TRACKED_STATE_ROOT_NAMESPACE,
        KvScanRange::prefix(Vec::new()),
    )
    .await?;
    (0..pairs.len())
        .map(|index| {
            let key = pairs.key(index).expect("scan row key exists").to_vec();
            let commit_id = String::from_utf8(key).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("tracked-state tree root key is invalid UTF-8: {error}"),
                )
            })?;
            let value = pairs.value_required(index)?;
            let root_id = TrackedStateRootId::from_slice(&value)?;
            Ok((commit_id, root_id))
        })
        .collect()
}

pub(crate) fn verify_chunk_hash(
    expected_hash: &[u8; TRACKED_STATE_HASH_BYTES],
    bytes: &[u8],
) -> Result<(), LixError> {
    let actual = crate::tracked_state::codec::hash_bytes(bytes);
    if &actual == expected_hash {
        return Ok(());
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "tracked-state tree chunk hash mismatch",
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::storage::StorageContext;

    #[tokio::test]
    async fn root_roundtrips_through_kv_storage() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let root = TrackedStateRootId::new([7_u8; TRACKED_STATE_HASH_BYTES]);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        store_root(&mut transaction.as_mut(), "commit-1", &root)
            .await
            .expect("root should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = storage.clone();
        assert_eq!(
            load_root(&mut store, "commit-1")
                .await
                .expect("root should load"),
            Some(root)
        );
    }

    #[tokio::test]
    async fn chunk_roundtrips_through_kv_storage() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let data = b"chunk-data".to_vec();
        let chunk = PendingChunkWrite {
            hash: crate::tracked_state::codec::hash_bytes(&data),
            data: data.clone(),
        };

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_chunks(&mut transaction.as_mut(), std::slice::from_ref(&chunk))
            .await
            .expect("chunk should write");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = storage.clone();
        assert_eq!(
            read_chunk(&mut store, &chunk.hash)
                .await
                .expect("chunk should read"),
            Some(data)
        );
    }

    #[tokio::test]
    async fn snapshot_roundtrips_raw_payload() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let snapshot_content = "{\"value\":\"small\"}";
        let encoded = encode_snapshot_content(snapshot_content).expect("snapshot should encode");
        assert_eq!(encoded.snapshot_ref.codec, SnapshotCodec::Raw);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        store_snapshot_ref(
            &mut transaction.as_mut(),
            &encoded.snapshot_ref,
            encoded.data.as_ref(),
        )
        .await
        .expect("snapshot should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = storage.clone();
        assert_eq!(
            load_snapshot(&mut store, &encoded.snapshot_ref)
                .await
                .expect("snapshot should load"),
            Some(snapshot_content.to_string())
        );
    }

    #[tokio::test]
    async fn snapshot_roundtrips_zstd_payload() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let snapshot_content = "zstd-friendly text ".repeat(2048);
        let encoded = encode_snapshot_content(&snapshot_content).expect("snapshot should encode");
        assert_eq!(encoded.snapshot_ref.codec, SnapshotCodec::Zstd);
        assert!(encoded.data.len() < snapshot_content.len());

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        store_snapshot_ref(
            &mut transaction.as_mut(),
            &encoded.snapshot_ref,
            encoded.data.as_ref(),
        )
        .await
        .expect("snapshot should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = storage.clone();
        assert_eq!(
            load_snapshot(&mut store, &encoded.snapshot_ref)
                .await
                .expect("snapshot should load"),
            Some(snapshot_content)
        );
    }

    #[tokio::test]
    async fn snapshot_roundtrips_json_chunks_and_shares_unchanged_chunks() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let before = large_json_array(None);
        let after = large_json_array(Some((128, "changed")));
        let expected_before = canonical_json(&before);
        let expected_after = canonical_json(&after);
        let encoded_before = encode_snapshot_content(&before).expect("snapshot should encode");
        let encoded_after = encode_snapshot_content(&after).expect("snapshot should encode");
        assert_eq!(encoded_before.snapshot_ref.codec, SnapshotCodec::JsonChunks);
        assert_eq!(encoded_after.snapshot_ref.codec, SnapshotCodec::JsonChunks);
        assert!(encoded_before.data.len() < before.len() / 4);

        let before_chunks = encoded_before
            .json_chunks
            .iter()
            .map(|chunk| chunk.hash_hex.as_str())
            .collect::<BTreeSet<_>>();
        let after_chunks = encoded_after
            .json_chunks
            .iter()
            .map(|chunk| chunk.hash_hex.as_str())
            .collect::<BTreeSet<_>>();
        let shared_chunks = before_chunks.intersection(&after_chunks).count();
        assert!(shared_chunks > before_chunks.len() / 2);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        store_encoded_snapshot(&mut transaction.as_mut(), &encoded_before)
            .await
            .expect("before snapshot should store");
        store_encoded_snapshot(&mut transaction.as_mut(), &encoded_after)
            .await
            .expect("after snapshot should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = storage.clone();
        assert_eq!(
            load_snapshot(&mut store, &encoded_before.snapshot_ref)
                .await
                .expect("before snapshot should load"),
            Some(expected_before)
        );
        assert_eq!(
            load_snapshot(&mut store, &encoded_after.snapshot_ref)
                .await
                .expect("after snapshot should load"),
            Some(expected_after)
        );
    }

    fn large_json_array(change: Option<(usize, &str)>) -> String {
        let items = (0..512)
            .map(|index| {
                let title = change
                    .filter(|(changed_index, _)| *changed_index == index)
                    .map(|(_, title)| title)
                    .unwrap_or("same");
                format!(
                    "{{\"id\":\"item-{index}\",\"kind\":\"task\",\"done\":false,\"title\":\"{title}\",\"body\":\"{}\"}}",
                    "body ".repeat(8)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("[{items}]")
    }

    fn canonical_json(value: &str) -> String {
        serde_json::to_string(&serde_json::from_str::<JsonValue>(value).expect("valid json"))
            .expect("json should serialize")
    }
}
