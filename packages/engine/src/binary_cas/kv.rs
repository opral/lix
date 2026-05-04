#![allow(dead_code)]

use crate::binary_cas::chunking::fastcdc_chunk_ranges;
use crate::binary_cas::codec::{
    binary_blob_hash_bytes, decode_binary_cas_chunk, decode_binary_cas_manifest,
    decode_binary_cas_manifest_chunk, encode_binary_cas_chunk, encode_binary_cas_manifest,
    encode_binary_cas_manifest_chunk, encode_binary_chunk_payload, hash_bytes_to_hex,
    hash_hex_to_bytes, BinaryCasManifest, BinaryChunkCodec,
};
use crate::binary_cas::BinaryBlobWrite;
use crate::storage::{
    KvGetGroup, KvGetRequest, KvPut, KvScanRange, KvScanRequest, KvWriteBatch, KvWriteGroup,
    StorageReader, StorageWriter,
};
use crate::LixError;
use std::collections::HashSet;

pub(crate) const BINARY_CAS_MANIFEST_NAMESPACE: &str = "binary_cas.manifest";
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_NAMESPACE: &str = "binary_cas.manifest_chunk";
pub(crate) const BINARY_CAS_CHUNK_NAMESPACE: &str = "binary_cas.chunk";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvBlobManifestChunk {
    pub(crate) chunk_hash: [u8; 32],
    pub(crate) chunk_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvChunk {
    pub(crate) codec: BinaryChunkCodec,
    pub(crate) uncompressed_len: u64,
    pub(crate) data: Vec<u8>,
}

pub(crate) async fn load_manifest(
    store: &mut impl StorageReader,
    blob_hash: &str,
) -> Result<Option<BinaryCasManifest>, LixError> {
    let Some(bytes) = get_one(
        store,
        BINARY_CAS_MANIFEST_NAMESPACE,
        manifest_key(blob_hash)?,
    )
    .await?
    else {
        return Ok(None);
    };
    decode_binary_cas_manifest(&bytes).map(Some)
}

#[cfg(feature = "storage-benches")]
pub(crate) async fn count_manifests(store: &mut impl StorageReader) -> Result<usize, LixError> {
    Ok(scan_all_values(
        store,
        BINARY_CAS_MANIFEST_NAMESPACE,
        KvScanRange::Prefix(Vec::new()),
    )
    .await?
    .len())
}

pub(crate) async fn put_manifest(
    writer: &mut impl StorageWriter,
    blob_hash: &str,
    manifest: &BinaryCasManifest,
) -> Result<(), LixError> {
    put_one(
        writer,
        BINARY_CAS_MANIFEST_NAMESPACE,
        manifest_key(blob_hash)?,
        encode_binary_cas_manifest(manifest),
    )
    .await
}

pub(crate) async fn scan_manifest_chunks(
    store: &mut impl StorageReader,
    blob_hash: &str,
) -> Result<Vec<KvBlobManifestChunk>, LixError> {
    scan_all_values(
        store,
        BINARY_CAS_MANIFEST_CHUNK_NAMESPACE,
        KvScanRange::Prefix(manifest_chunk_prefix(blob_hash)?),
    )
    .await?
    .into_iter()
    .map(|value| {
        let (chunk_hash, chunk_size) = decode_binary_cas_manifest_chunk(&value)?;
        Ok(KvBlobManifestChunk {
            chunk_hash,
            chunk_size,
        })
    })
    .collect()
}

pub(crate) async fn put_manifest_chunk(
    writer: &mut impl StorageWriter,
    blob_hash: &str,
    chunk_index: u64,
    chunk: &KvBlobManifestChunk,
) -> Result<(), LixError> {
    put_one(
        writer,
        BINARY_CAS_MANIFEST_CHUNK_NAMESPACE,
        manifest_chunk_key(blob_hash, chunk_index)?,
        encode_binary_cas_manifest_chunk(&chunk.chunk_hash, chunk.chunk_size),
    )
    .await
}

pub(crate) async fn load_chunk(
    store: &mut impl StorageReader,
    chunk_hash: &str,
) -> Result<Option<KvChunk>, LixError> {
    let Some(bytes) = get_one(store, BINARY_CAS_CHUNK_NAMESPACE, chunk_key(chunk_hash)?).await?
    else {
        return Ok(None);
    };
    let (codec, uncompressed_len, payload) = decode_binary_cas_chunk(&bytes)?;
    Ok(Some(KvChunk {
        codec,
        uncompressed_len,
        data: payload.to_vec(),
    }))
}

pub(crate) async fn put_chunk(
    writer: &mut impl StorageWriter,
    chunk_hash: &str,
    chunk: &KvChunk,
) -> Result<(), LixError> {
    put_one(
        writer,
        BINARY_CAS_CHUNK_NAMESPACE,
        chunk_key(chunk_hash)?,
        encode_binary_cas_chunk(chunk.codec, chunk.uncompressed_len, &chunk.data),
    )
    .await
}

async fn get_one(
    store: &mut impl StorageReader,
    namespace: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    Ok(store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: namespace.to_string(),
                keys: vec![key],
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .and_then(|mut group| group.pop_value()))
}

async fn scan_all_values(
    store: &mut impl StorageReader,
    namespace: &str,
    range: KvScanRange,
) -> Result<Vec<Vec<u8>>, LixError> {
    let page = store
        .scan_values(KvScanRequest {
            namespace: namespace.to_string(),
            range,
            after: None,
            limit: usize::MAX,
        })
        .await?
        .values;
    Ok(page.iter().map(<[u8]>::to_vec).collect())
}

async fn put_one(
    writer: &mut impl StorageWriter,
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

pub(crate) async fn load_blob_data_by_hash(
    store: &mut impl StorageReader,
    blob_hash: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let requested_blob_hash = hash_hex_to_bytes(blob_hash, "binary CAS blob")?;
    let Some(manifest) = load_manifest(store, blob_hash).await? else {
        return Ok(None);
    };
    let expected_blob_size = persisted_size_to_usize(manifest.size_bytes(), "binary CAS blob")?;
    match manifest {
        BinaryCasManifest::Empty { size_bytes } => {
            if size_bytes != 0 {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("binary CAS empty blob '{blob_hash}' has nonzero size {size_bytes}"),
                ));
            }
            if requested_blob_hash != binary_blob_hash_bytes(&[]) {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("binary CAS blob '{blob_hash}' failed content-address verification"),
                ));
            }
            return Ok(Some(Vec::new()));
        }
        BinaryCasManifest::SingleChunk {
            size_bytes,
            chunk_hash,
        } => {
            return load_single_chunk_blob(
                store,
                blob_hash,
                requested_blob_hash,
                chunk_hash,
                size_bytes,
            )
            .await
            .map(Some);
        }
        BinaryCasManifest::Chunked { chunk_count, .. } => {
            return load_chunked_blob(
                store,
                blob_hash,
                requested_blob_hash,
                expected_blob_size,
                chunk_count,
            )
            .await
            .map(Some);
        }
    }
}

async fn load_single_chunk_blob(
    store: &mut impl StorageReader,
    blob_hash: &str,
    requested_blob_hash: [u8; 32],
    chunk_hash: [u8; 32],
    size_bytes: u64,
) -> Result<Vec<u8>, LixError> {
    let expected_chunk_size = persisted_size_to_usize(size_bytes, "binary CAS chunk")?;
    let chunk_row = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: BINARY_CAS_CHUNK_NAMESPACE.to_string(),
                keys: vec![chunk_hash.to_vec()],
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .and_then(|mut group| group.pop_value())
        .ok_or_else(|| {
            let chunk_hash = hash_bytes_to_hex(&chunk_hash);
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("binary CAS chunk '{chunk_hash}' is missing for blob '{blob_hash}'"),
            )
        })?;
    let chunk_hash_hex = hash_bytes_to_hex(&chunk_hash);
    let payload = decode_and_verify_chunk(
        &chunk_row,
        expected_chunk_size,
        blob_hash,
        &chunk_hash_hex,
        chunk_hash,
    )?;
    if chunk_hash != requested_blob_hash && binary_blob_hash_bytes(&payload) != requested_blob_hash
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("binary CAS blob '{blob_hash}' failed content-address verification"),
        ));
    }
    Ok(payload)
}

async fn load_chunked_blob(
    store: &mut impl StorageReader,
    blob_hash: &str,
    requested_blob_hash: [u8; 32],
    expected_blob_size: usize,
    chunk_count: u32,
) -> Result<Vec<u8>, LixError> {
    let manifest_chunks = scan_manifest_chunks(store, blob_hash).await?;
    if manifest_chunks.len() != chunk_count as usize {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS blob '{}' expected {} chunks, found {}",
                blob_hash,
                chunk_count,
                manifest_chunks.len()
            ),
        ));
    }

    let mut out = Vec::with_capacity(expected_blob_size);
    let chunk_keys = manifest_chunks
        .iter()
        .map(|manifest_chunk| manifest_chunk.chunk_hash.to_vec())
        .collect::<Vec<_>>();
    let chunk_rows = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: BINARY_CAS_CHUNK_NAMESPACE.to_string(),
                keys: chunk_keys,
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .map(|group| group.values)
        .unwrap_or_default();
    if chunk_rows.len() != manifest_chunks.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS blob '{}' expected {} chunk rows, got {}",
                blob_hash,
                manifest_chunks.len(),
                chunk_rows.len()
            ),
        ));
    }

    for (index, manifest_chunk) in manifest_chunks.iter().enumerate() {
        let Some(chunk_bytes) = chunk_rows[index].as_deref() else {
            let chunk_hash = hash_bytes_to_hex(&manifest_chunk.chunk_hash);
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "binary CAS chunk '{}' is missing for blob '{}'",
                    chunk_hash, blob_hash
                ),
            ));
        };
        let expected_chunk_size =
            persisted_size_to_usize(manifest_chunk.chunk_size, "binary CAS chunk")?;
        let chunk_hash = hash_bytes_to_hex(&manifest_chunk.chunk_hash);
        let payload = decode_and_verify_chunk(
            chunk_bytes,
            expected_chunk_size,
            blob_hash,
            &chunk_hash,
            manifest_chunk.chunk_hash,
        )?;
        out.extend_from_slice(&payload);
    }

    if out.len() != expected_blob_size {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS blob '{}' expected {} bytes, decoded {} bytes",
                blob_hash,
                expected_blob_size,
                out.len()
            ),
        ));
    }
    if binary_blob_hash_bytes(&out) != requested_blob_hash {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("binary CAS blob '{blob_hash}' failed content-address verification"),
        ));
    }
    Ok(out)
}

fn decode_and_verify_chunk(
    chunk_bytes: &[u8],
    expected_chunk_size: usize,
    blob_hash: &str,
    chunk_hash_hex: &str,
    chunk_hash: [u8; 32],
) -> Result<Vec<u8>, LixError> {
    let (codec, uncompressed_len, chunk_payload) = decode_binary_cas_chunk(chunk_bytes)?;
    if uncompressed_len != expected_chunk_size as u64 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' for blob '{}' expected {} uncompressed bytes, row says {}",
                chunk_hash_hex, blob_hash, expected_chunk_size, uncompressed_len
            ),
        ));
    }
    let BinaryChunkCodec::Raw = codec;
    if chunk_payload.len() != expected_chunk_size {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' for blob '{}' expected {} decoded bytes, got {}",
                chunk_hash_hex,
                blob_hash,
                expected_chunk_size,
                chunk_payload.len()
            ),
        ));
    }
    if binary_blob_hash_bytes(chunk_payload) != chunk_hash {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' for blob '{}' failed content-address verification",
                chunk_hash_hex, blob_hash
            ),
        ));
    }
    Ok(chunk_payload.to_vec())
}

pub(crate) async fn blob_exists(
    store: &mut impl StorageReader,
    blob_hash: &str,
) -> Result<bool, LixError> {
    Ok(load_manifest(store, blob_hash).await?.is_some())
}

pub(crate) async fn persist_blob_writes_in_transaction(
    writer: &mut impl StorageWriter,
    writes: &[BinaryBlobWrite<'_>],
) -> Result<(), LixError> {
    let mut batch = KvWriteBatch::new();
    let mut blob_hashes = HashSet::new();
    let mut chunk_keys = HashSet::new();

    for write in writes {
        stage_blob_write(&mut batch, &mut blob_hashes, &mut chunk_keys, write)?;
    }
    if !batch.is_empty() {
        writer.write_kv_batch(batch).await?;
    }
    Ok(())
}

fn stage_blob_write(
    batch: &mut KvWriteBatch,
    blob_hashes: &mut HashSet<[u8; 32]>,
    chunk_keys: &mut HashSet<Vec<u8>>,
    write: &BinaryBlobWrite<'_>,
) -> Result<(), LixError> {
    let blob_hash = binary_blob_hash_bytes(write.data);
    if !blob_hashes.insert(blob_hash) {
        return Ok(());
    }

    let chunk_ranges = fastcdc_chunk_ranges(write.data);
    let manifest_key = blob_hash.to_vec();
    match chunk_ranges.as_slice() {
        [] => {
            batch.put(
                BINARY_CAS_MANIFEST_NAMESPACE,
                manifest_key,
                encode_binary_cas_manifest(&BinaryCasManifest::Empty { size_bytes: 0 }),
            );
        }
        [(start, end)] => {
            let chunk_data = &write.data[*start..*end];
            let chunk_hash = binary_blob_hash_bytes(chunk_data);
            let chunk_key = chunk_hash.to_vec();
            batch.put(
                BINARY_CAS_MANIFEST_NAMESPACE,
                manifest_key,
                encode_binary_cas_manifest(&BinaryCasManifest::SingleChunk {
                    size_bytes: write.data.len() as u64,
                    chunk_hash,
                }),
            );
            if chunk_keys.insert(chunk_key.clone()) {
                let encoded_chunk = encode_binary_chunk_payload(chunk_data);
                batch.put(
                    BINARY_CAS_CHUNK_NAMESPACE,
                    chunk_key,
                    encode_binary_cas_chunk(
                        encoded_chunk.codec,
                        chunk_data.len() as u64,
                        &encoded_chunk.data,
                    ),
                );
            }
        }
        _ => {
            let manifest = BinaryCasManifest::Chunked {
                size_bytes: write.data.len() as u64,
                chunk_count: u32::try_from(chunk_ranges.len()).map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "binary CAS blob has too many chunks for manifest".to_string(),
                    )
                })?,
            };
            batch.put(
                BINARY_CAS_MANIFEST_NAMESPACE,
                manifest_key,
                encode_binary_cas_manifest(&manifest),
            );

            for (chunk_index, (start, end)) in chunk_ranges.into_iter().enumerate() {
                let chunk_data = &write.data[start..end];
                let chunk_hash = binary_blob_hash_bytes(chunk_data);
                let chunk_key = chunk_hash.to_vec();
                if chunk_keys.insert(chunk_key.clone()) {
                    let encoded_chunk = encode_binary_chunk_payload(chunk_data);
                    batch.put(
                        BINARY_CAS_CHUNK_NAMESPACE,
                        chunk_key,
                        encode_binary_cas_chunk(
                            encoded_chunk.codec,
                            chunk_data.len() as u64,
                            &encoded_chunk.data,
                        ),
                    );
                }

                batch.put(
                    BINARY_CAS_MANIFEST_CHUNK_NAMESPACE,
                    manifest_chunk_key_bytes(&blob_hash, chunk_index as u64),
                    encode_binary_cas_manifest_chunk(&chunk_hash, chunk_data.len() as u64),
                );
            }
        }
    }
    Ok(())
}

fn manifest_key(blob_hash: &str) -> Result<Vec<u8>, LixError> {
    Ok(hash_hex_to_bytes(blob_hash, "binary CAS blob")?.to_vec())
}

fn manifest_chunk_prefix(blob_hash: &str) -> Result<Vec<u8>, LixError> {
    Ok(hash_hex_to_bytes(blob_hash, "binary CAS blob")?.to_vec())
}

fn manifest_chunk_key(blob_hash: &str, chunk_index: u64) -> Result<Vec<u8>, LixError> {
    Ok(manifest_chunk_key_bytes(
        &hash_hex_to_bytes(blob_hash, "binary CAS blob")?,
        chunk_index,
    ))
}

fn manifest_chunk_key_bytes(blob_hash: &[u8; 32], chunk_index: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    out.extend_from_slice(blob_hash);
    out.extend_from_slice(&chunk_index.to_be_bytes());
    out
}

fn chunk_key(chunk_hash: &str) -> Result<Vec<u8>, LixError> {
    Ok(hash_hex_to_bytes(chunk_hash, "binary CAS chunk")?.to_vec())
}

fn persisted_size_to_usize(size: u64, label: &str) -> Result<usize, LixError> {
    usize::try_from(size).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{label} size {size} does not fit in this runtime"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::binary_cas::codec::binary_blob_hash_hex;
    use crate::storage::StorageContext;

    #[tokio::test]
    async fn stores_manifest_chunks_in_scan_order() {
        let storage = StorageContext::new(std::sync::Arc::new(UnitTestBackend::new()));
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let blob_hash = binary_blob_hash_hex(b"blob-a");
        let chunk_a_hash = binary_blob_hash_bytes(b"chunk-a");
        let chunk_b_hash = binary_blob_hash_bytes(b"chunk-b");

        {
            let mut writer = transaction.as_mut();
            put_manifest(
                &mut writer,
                &blob_hash,
                &BinaryCasManifest::Chunked {
                    size_bytes: 12,
                    chunk_count: 2,
                },
            )
            .await
            .expect("manifest should persist");
            put_manifest_chunk(
                &mut writer,
                &blob_hash,
                1,
                &KvBlobManifestChunk {
                    chunk_hash: chunk_b_hash,
                    chunk_size: 6,
                },
            )
            .await
            .expect("chunk ref should persist");
            put_manifest_chunk(
                &mut writer,
                &blob_hash,
                0,
                &KvBlobManifestChunk {
                    chunk_hash: chunk_a_hash,
                    chunk_size: 6,
                },
            )
            .await
            .expect("chunk ref should persist");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut store = storage.clone();
        assert_eq!(
            load_manifest(&mut store, &blob_hash)
                .await
                .expect("manifest should load"),
            Some(BinaryCasManifest::Chunked {
                size_bytes: 12,
                chunk_count: 2,
            })
        );
        let mut store = storage.clone();
        assert_eq!(
            scan_manifest_chunks(&mut store, &blob_hash)
                .await
                .expect("manifest chunks should scan"),
            vec![
                KvBlobManifestChunk {
                    chunk_hash: chunk_a_hash,
                    chunk_size: 6,
                },
                KvBlobManifestChunk {
                    chunk_hash: chunk_b_hash,
                    chunk_size: 6,
                },
            ]
        );
    }

    #[tokio::test]
    async fn stores_encoded_chunks_by_chunk_hash() {
        let storage = StorageContext::new(std::sync::Arc::new(UnitTestBackend::new()));
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let chunk = KvChunk {
            codec: BinaryChunkCodec::Raw,
            uncompressed_len: 5,
            data: b"hello".to_vec(),
        };
        let chunk_hash = binary_blob_hash_hex(b"chunk-a");

        {
            let mut writer = transaction.as_mut();
            put_chunk(&mut writer, &chunk_hash, &chunk)
                .await
                .expect("chunk should persist");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut store = storage.clone();
        assert_eq!(
            load_chunk(&mut store, &chunk_hash)
                .await
                .expect("chunk should load"),
            Some(chunk)
        );
    }

    #[test]
    fn binary_hash_keys_are_compact_and_manifest_chunks_sort_by_index() {
        let blob_hash = binary_blob_hash_hex(b"blob");
        let manifest_key = manifest_key(&blob_hash).expect("manifest key should encode");
        let chunk_key =
            chunk_key(&binary_blob_hash_hex(b"chunk")).expect("chunk key should encode");
        let first = manifest_chunk_key(&blob_hash, 1).expect("first key should encode");
        let second = manifest_chunk_key(&blob_hash, 2).expect("second key should encode");
        let later = manifest_chunk_key(&blob_hash, 10).expect("later key should encode");

        assert_eq!(manifest_key.len(), 32);
        assert_eq!(chunk_key.len(), 32);
        assert_eq!(first.len(), 40);
        assert!(first < second);
        assert!(second < later);
    }

    #[tokio::test]
    async fn public_kv_api_roundtrips_blob_bytes() {
        let storage = StorageContext::new(std::sync::Arc::new(UnitTestBackend::new()));
        let data = b"hello chunked kv cas";
        let blob_hash = binary_blob_hash_hex(data);
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        {
            let mut writer = transaction.as_mut();
            persist_blob_writes_in_transaction(
                &mut writer,
                &[BinaryBlobWrite {
                    file_id: "file-a",
                    version_id: "global",
                    data,
                }],
            )
            .await
            .expect("blob write should persist");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut store = storage.clone();
        assert_eq!(
            load_blob_data_by_hash(&mut store, &blob_hash)
                .await
                .expect("blob should load"),
            Some(data.to_vec())
        );
        let mut store = storage.clone();
        assert_eq!(
            load_manifest(&mut store, &blob_hash)
                .await
                .expect("manifest should load"),
            Some(BinaryCasManifest::SingleChunk {
                size_bytes: data.len() as u64,
                chunk_hash: binary_blob_hash_bytes(data),
            })
        );
        let mut store = storage.clone();
        assert_eq!(
            scan_manifest_chunks(&mut store, &blob_hash)
                .await
                .expect("single-chunk blob should not spill manifest chunks"),
            Vec::<KvBlobManifestChunk>::new()
        );
        let mut store = storage.clone();
        assert!(blob_exists(&mut store, &blob_hash)
            .await
            .expect("blob exists should succeed"));
    }

    #[tokio::test]
    async fn read_rejects_chunk_bytes_that_do_not_match_manifest_hash() {
        let storage = StorageContext::new(std::sync::Arc::new(UnitTestBackend::new()));
        let data = b"same length";
        let corrupted = b"SAME length";
        let blob_hash = binary_blob_hash_hex(data);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let mut writer = transaction.as_mut();
            persist_blob_writes_in_transaction(
                &mut writer,
                &[BinaryBlobWrite {
                    file_id: "file-corrupt-chunk",
                    version_id: "global",
                    data,
                }],
            )
            .await
            .expect("blob write should persist");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let mut writer = transaction.as_mut();
            put_one(
                &mut writer,
                BINARY_CAS_CHUNK_NAMESPACE,
                chunk_key(&blob_hash).expect("chunk key should encode"),
                encode_binary_cas_chunk(BinaryChunkCodec::Raw, corrupted.len() as u64, corrupted),
            )
            .await
            .expect("corrupt chunk should overwrite");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut store = storage.clone();
        let error = load_blob_data_by_hash(&mut store, &blob_hash)
            .await
            .expect_err("corrupt chunk should be rejected");
        assert!(error
            .message
            .contains("failed content-address verification"));
    }

    #[tokio::test]
    async fn read_rejects_manifest_that_assembles_wrong_blob_hash() {
        let storage = StorageContext::new(std::sync::Arc::new(UnitTestBackend::new()));
        let expected = b"expected bytes";
        let substituted = b"different byte";
        assert_eq!(expected.len(), substituted.len());
        let expected_blob_hash = binary_blob_hash_hex(expected);
        let substituted_chunk_hash = binary_blob_hash_hex(substituted);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        {
            let mut writer = transaction.as_mut();
            put_manifest(
                &mut writer,
                &expected_blob_hash,
                &BinaryCasManifest::Chunked {
                    size_bytes: expected.len() as u64,
                    chunk_count: 1,
                },
            )
            .await
            .expect("manifest should persist");
            put_manifest_chunk(
                &mut writer,
                &expected_blob_hash,
                0,
                &KvBlobManifestChunk {
                    chunk_hash: binary_blob_hash_bytes(substituted),
                    chunk_size: substituted.len() as u64,
                },
            )
            .await
            .expect("manifest chunk should persist");
            put_chunk(
                &mut writer,
                &substituted_chunk_hash,
                &KvChunk {
                    codec: BinaryChunkCodec::Raw,
                    uncompressed_len: substituted.len() as u64,
                    data: substituted.to_vec(),
                },
            )
            .await
            .expect("chunk should persist");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut store = storage.clone();
        let error = load_blob_data_by_hash(&mut store, &expected_blob_hash)
            .await
            .expect_err("wrong assembled blob should be rejected");
        assert!(error
            .message
            .contains("failed content-address verification"));
    }

    #[tokio::test]
    async fn public_kv_api_roundtrips_empty_blob() {
        let storage = StorageContext::new(std::sync::Arc::new(UnitTestBackend::new()));
        let data = b"";
        let blob_hash = binary_blob_hash_hex(data);
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        {
            let mut writer = transaction.as_mut();
            persist_blob_writes_in_transaction(
                &mut writer,
                &[BinaryBlobWrite {
                    file_id: "file-empty",
                    version_id: "global",
                    data,
                }],
            )
            .await
            .expect("empty blob write should persist");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut store = storage.clone();
        assert_eq!(
            load_blob_data_by_hash(&mut store, &blob_hash)
                .await
                .expect("empty blob should load"),
            Some(Vec::new())
        );
        let mut store = storage.clone();
        assert_eq!(
            scan_manifest_chunks(&mut store, &blob_hash)
                .await
                .expect("empty blob chunks should scan"),
            Vec::<KvBlobManifestChunk>::new()
        );
    }

    #[tokio::test]
    async fn public_kv_api_roundtrips_multi_chunk_blob() {
        let storage = StorageContext::new(std::sync::Arc::new(UnitTestBackend::new()));
        let data = (0..600_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let blob_hash = binary_blob_hash_hex(&data);
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        {
            let mut writer = transaction.as_mut();
            persist_blob_writes_in_transaction(
                &mut writer,
                &[BinaryBlobWrite {
                    file_id: "file-large",
                    version_id: "global",
                    data: &data,
                }],
            )
            .await
            .expect("large blob write should persist");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut store = storage.clone();
        assert_eq!(
            load_blob_data_by_hash(&mut store, &blob_hash)
                .await
                .expect("large blob should load"),
            Some(data)
        );
        let mut store = storage.clone();
        assert!(
            scan_manifest_chunks(&mut store, &blob_hash)
                .await
                .expect("large blob chunks should scan")
                .len()
                > 1
        );
    }
}
