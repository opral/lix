#![allow(dead_code)]

use crate::binary_cas::chunking::fastcdc_chunk_ranges;
use crate::binary_cas::codec::{
    binary_blob_hash_hex, decode_binary_chunk_payload, encode_binary_chunk_payload,
};
use crate::binary_cas::BinaryBlobWrite;
use crate::storage::{
    KvGetGroup, KvGetProjection, KvGetRequest, KvPut, KvScanProjection, KvScanRange, KvScanRequest,
    KvWriteBatch, KvWriteGroup, StorageReader, StorageWriter,
};
use crate::LixError;

pub(crate) const BINARY_CAS_MANIFEST_NAMESPACE: &str = "binary_cas.manifest";
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_NAMESPACE: &str = "binary_cas.manifest_chunk";
pub(crate) const BINARY_CAS_CHUNK_NAMESPACE: &str = "binary_cas.chunk";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct KvBlobManifest {
    pub(crate) size_bytes: u64,
    pub(crate) chunk_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct KvBlobManifestChunk {
    pub(crate) chunk_hash: String,
    pub(crate) chunk_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct KvChunk {
    pub(crate) codec: String,
    pub(crate) codec_dict_id: Option<String>,
    pub(crate) data: Vec<u8>,
}

pub(crate) async fn load_manifest(
    store: &mut impl StorageReader,
    blob_hash: &str,
) -> Result<Option<KvBlobManifest>, LixError> {
    let Some(bytes) = get_one(
        store,
        BINARY_CAS_MANIFEST_NAMESPACE,
        manifest_key(blob_hash),
    )
    .await?
    else {
        return Ok(None);
    };
    decode_json(&bytes, "binary CAS manifest").map(Some)
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
    manifest: &KvBlobManifest,
) -> Result<(), LixError> {
    put_one(
        writer,
        BINARY_CAS_MANIFEST_NAMESPACE,
        manifest_key(blob_hash),
        encode_json(manifest, "binary CAS manifest")?,
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
        KvScanRange::Prefix(manifest_chunk_prefix(blob_hash)),
    )
    .await?
    .into_iter()
    .map(|value| decode_json(&value, "binary CAS manifest chunk"))
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
        manifest_chunk_key(blob_hash, chunk_index),
        encode_json(chunk, "binary CAS manifest chunk")?,
    )
    .await
}

pub(crate) async fn load_chunk(
    store: &mut impl StorageReader,
    chunk_hash: &str,
) -> Result<Option<KvChunk>, LixError> {
    let Some(bytes) = get_one(store, BINARY_CAS_CHUNK_NAMESPACE, chunk_key(chunk_hash)).await?
    else {
        return Ok(None);
    };
    decode_json(&bytes, "binary CAS chunk").map(Some)
}

pub(crate) async fn put_chunk(
    writer: &mut impl StorageWriter,
    chunk_hash: &str,
    chunk: &KvChunk,
) -> Result<(), LixError> {
    put_one(
        writer,
        BINARY_CAS_CHUNK_NAMESPACE,
        chunk_key(chunk_hash),
        encode_json(chunk, "binary CAS chunk")?,
    )
    .await
}

async fn get_one(
    store: &mut impl StorageReader,
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

async fn scan_all_values(
    store: &mut impl StorageReader,
    namespace: &str,
    range: KvScanRange,
) -> Result<Vec<Vec<u8>>, LixError> {
    Ok(store
        .scan_kv(KvScanRequest {
            namespace: namespace.to_string(),
            range,
            after: None,
            limit: usize::MAX,
            projection: KvScanProjection::KeysAndValues,
        })
        .await?
        .into_rows()
        .into_values_required()?)
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
    let Some(manifest) = load_manifest(store, blob_hash).await? else {
        return Ok(None);
    };
    let manifest_chunks = scan_manifest_chunks(store, blob_hash).await?;
    if manifest_chunks.len() != manifest.chunk_count as usize {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS blob '{}' expected {} chunks, found {}",
                blob_hash,
                manifest.chunk_count,
                manifest_chunks.len()
            ),
        ));
    }

    let mut out = Vec::with_capacity(manifest.size_bytes as usize);
    for manifest_chunk in manifest_chunks {
        let Some(chunk) = load_chunk(store, &manifest_chunk.chunk_hash).await? else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "binary CAS chunk '{}' is missing for blob '{}'",
                    manifest_chunk.chunk_hash, blob_hash
                ),
            ));
        };
        let decoded = decode_binary_chunk_payload(
            &chunk.data,
            Some(&chunk.codec),
            manifest_chunk.chunk_size as usize,
            blob_hash,
            &manifest_chunk.chunk_hash,
            "binary CAS KV read",
        )?;
        out.extend_from_slice(&decoded);
    }

    if out.len() != manifest.size_bytes as usize {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS blob '{}' expected {} bytes, decoded {} bytes",
                blob_hash,
                manifest.size_bytes,
                out.len()
            ),
        ));
    }
    Ok(Some(out))
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
    for write in writes {
        persist_one_blob_write(writer, write).await?;
    }
    Ok(())
}

async fn persist_one_blob_write(
    writer: &mut impl StorageWriter,
    write: &BinaryBlobWrite<'_>,
) -> Result<(), LixError> {
    let blob_hash = binary_blob_hash_hex(write.data);
    let chunk_ranges = fastcdc_chunk_ranges(write.data);
    put_manifest(
        writer,
        &blob_hash,
        &KvBlobManifest {
            size_bytes: write.data.len() as u64,
            chunk_count: chunk_ranges.len() as u64,
        },
    )
    .await?;

    for (chunk_index, (start, end)) in chunk_ranges.into_iter().enumerate() {
        let chunk_data = &write.data[start..end];
        let encoded_chunk = encode_binary_chunk_payload(chunk_data)?;
        let chunk_hash = binary_blob_hash_hex(chunk_data);
        put_chunk(
            writer,
            &chunk_hash,
            &KvChunk {
                codec: encoded_chunk.codec.to_string(),
                codec_dict_id: encoded_chunk.codec_dict_id,
                data: encoded_chunk.data,
            },
        )
        .await?;
        put_manifest_chunk(
            writer,
            &blob_hash,
            chunk_index as u64,
            &KvBlobManifestChunk {
                chunk_hash,
                chunk_size: chunk_data.len() as u64,
            },
        )
        .await?;
    }
    Ok(())
}

fn manifest_key(blob_hash: &str) -> Vec<u8> {
    blob_hash.as_bytes().to_vec()
}

fn manifest_chunk_prefix(blob_hash: &str) -> Vec<u8> {
    format!("{blob_hash}/").into_bytes()
}

fn manifest_chunk_key(blob_hash: &str, chunk_index: u64) -> Vec<u8> {
    // Fixed-width decimal preserves chunk order under lexicographic KV scans.
    format!("{blob_hash}/{chunk_index:020}").into_bytes()
}

fn chunk_key(chunk_hash: &str) -> Vec<u8> {
    chunk_hash.as_bytes().to_vec()
}

fn encode_json<T: serde::Serialize>(value: &T, label: &str) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(value).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to encode {label}: {error}"),
        )
    })
}

fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8], label: &str) -> Result<T, LixError> {
    serde_json::from_slice(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode {label}: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::storage::StorageContext;

    #[tokio::test]
    async fn stores_manifest_chunks_in_scan_order() {
        let storage = StorageContext::new(std::sync::Arc::new(UnitTestBackend::new()));
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        {
            let mut writer = transaction.as_mut();
            put_manifest(
                &mut writer,
                "blob-a",
                &KvBlobManifest {
                    size_bytes: 12,
                    chunk_count: 2,
                },
            )
            .await
            .expect("manifest should persist");
            put_manifest_chunk(
                &mut writer,
                "blob-a",
                1,
                &KvBlobManifestChunk {
                    chunk_hash: "chunk-b".to_string(),
                    chunk_size: 6,
                },
            )
            .await
            .expect("chunk ref should persist");
            put_manifest_chunk(
                &mut writer,
                "blob-a",
                0,
                &KvBlobManifestChunk {
                    chunk_hash: "chunk-a".to_string(),
                    chunk_size: 6,
                },
            )
            .await
            .expect("chunk ref should persist");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut store = storage.clone();
        assert_eq!(
            load_manifest(&mut store, "blob-a")
                .await
                .expect("manifest should load"),
            Some(KvBlobManifest {
                size_bytes: 12,
                chunk_count: 2,
            })
        );
        let mut store = storage.clone();
        assert_eq!(
            scan_manifest_chunks(&mut store, "blob-a")
                .await
                .expect("manifest chunks should scan"),
            vec![
                KvBlobManifestChunk {
                    chunk_hash: "chunk-a".to_string(),
                    chunk_size: 6,
                },
                KvBlobManifestChunk {
                    chunk_hash: "chunk-b".to_string(),
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
            codec: "raw".to_string(),
            codec_dict_id: None,
            data: b"hello".to_vec(),
        };

        {
            let mut writer = transaction.as_mut();
            put_chunk(&mut writer, "chunk-a", &chunk)
                .await
                .expect("chunk should persist");
        }
        transaction.commit().await.expect("commit should succeed");

        let mut store = storage.clone();
        assert_eq!(
            load_chunk(&mut store, "chunk-a")
                .await
                .expect("chunk should load"),
            Some(chunk)
        );
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
        assert!(blob_exists(&mut store, &blob_hash)
            .await
            .expect("blob exists should succeed"));
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
