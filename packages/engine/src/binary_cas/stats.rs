use std::ops::Bound;

use crate::LixError;
use crate::binary_cas::codec::{BinaryCasManifest, decode_binary_cas_manifest};
use crate::binary_cas::kv::{
    BINARY_CAS_CHUNK_PRESENCE_SPACE, BINARY_CAS_CHUNK_SPACE, BINARY_CAS_MANIFEST_CHUNK_SPACE,
    BINARY_CAS_MANIFEST_SPACE,
};
use crate::storage_adapter::{
    StorageAdapterRead, StorageCoreProjection, StorageError, StorageKeyRange,
    StorageProjectedValue, StorageScanOptions, StorageSpaceId,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BinaryCasStorageStats {
    pub manifest_rows: u64,
    pub empty_blob_rows: u64,
    pub single_chunk_blob_rows: u64,
    pub chunked_blob_rows: u64,
    pub manifest_chunk_rows: u64,
    pub chunk_presence_rows: u64,
    pub chunk_rows: u64,
    pub total_chunk_refs: u64,
    pub logical_blob_bytes: u64,
}

pub(crate) async fn collect_binary_cas_storage_stats<R>(
    read: &R,
) -> Result<BinaryCasStorageStats, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut stats = BinaryCasStorageStats::default();
    stats.manifest_rows = scan_space(
        read,
        BINARY_CAS_MANIFEST_SPACE.id,
        StorageCoreProjection::FullValue,
        |value| {
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(StorageError::Corruption(
                    "binary CAS manifest scan returned key-only value".to_string(),
                ));
            };
            let manifest = decode_binary_cas_manifest(&bytes).map_err(|error| {
                StorageError::Corruption(format!("invalid binary CAS manifest: {error}"))
            })?;
            stats.logical_blob_bytes += manifest.size_bytes();
            match manifest {
                BinaryCasManifest::Empty { .. } => stats.empty_blob_rows += 1,
                BinaryCasManifest::SingleChunk { .. } => {
                    stats.single_chunk_blob_rows += 1;
                    stats.total_chunk_refs += 1;
                }
                BinaryCasManifest::Chunked { chunk_count, .. } => {
                    stats.chunked_blob_rows += 1;
                    stats.total_chunk_refs += u64::from(chunk_count);
                }
            }
            Ok(())
        },
    )
    .await?;
    stats.manifest_chunk_rows = count_space(read, BINARY_CAS_MANIFEST_CHUNK_SPACE.id).await?;
    stats.chunk_presence_rows = count_space(read, BINARY_CAS_CHUNK_PRESENCE_SPACE.id).await?;
    stats.chunk_rows = count_space(read, BINARY_CAS_CHUNK_SPACE.id).await?;
    Ok(stats)
}

async fn count_space<R>(read: &R, space: StorageSpaceId) -> Result<u64, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    scan_space(read, space, StorageCoreProjection::KeyOnly, |_| Ok(())).await
}

async fn scan_space<R, F>(
    read: &R,
    space: StorageSpaceId,
    projection: StorageCoreProjection,
    mut visit: F,
) -> Result<u64, StorageError>
where
    R: StorageAdapterRead + ?Sized,
    F: FnMut(StorageProjectedValue) -> Result<(), StorageError>,
{
    let range = StorageKeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let mut resume_after = None;
    let mut row_count = 0_u64;

    loop {
        let result = read
            .scan(
                space,
                range.clone(),
                StorageScanOptions {
                    projection,
                    limit_rows: 4096,
                    resume_after,
                },
            )
            .await?;
        row_count += result.entries.len() as u64;
        let last_key = result.entries.last().map(|entry| entry.key.clone());
        for entry in result.entries {
            visit(entry.value)?;
        }
        if !result.has_more || last_key.is_none() {
            break;
        }
        resume_after = last_key;
    }

    Ok(row_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary_cas::BlobHash;
    use crate::binary_cas::codec::BinaryChunkCodec;
    use crate::binary_cas::kv::{
        KvBlobManifestChunk, stage_chunk, stage_manifest, stage_manifest_chunk,
    };
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    #[tokio::test]
    async fn counts_binary_cas_storage_rows() {
        let storage = Memory::new();
        let storage = StorageAdapter::new(storage);
        let mut writes = storage.new_write_set();

        let empty_hash = BlobHash::from_content(b"empty");
        stage_manifest(
            &mut writes,
            empty_hash,
            &BinaryCasManifest::Empty { size_bytes: 0 },
        );

        let single_hash = BlobHash::from_content(b"single blob");
        let single_chunk_hash = BlobHash::from_content(b"single");
        stage_manifest(
            &mut writes,
            single_hash,
            &BinaryCasManifest::SingleChunk {
                size_bytes: 6,
                chunk_hash: single_chunk_hash.into_bytes(),
            },
        );
        stage_chunk(
            &mut writes,
            single_chunk_hash,
            BinaryChunkCodec::Raw,
            6,
            b"single",
        );

        let chunked_hash = BlobHash::from_content(b"chunked blob");
        stage_manifest(
            &mut writes,
            chunked_hash,
            &BinaryCasManifest::Chunked {
                size_bytes: 8,
                chunk_count: 2,
            },
        );
        for (index, payload) in [b"left".as_slice(), b"side".as_slice()]
            .into_iter()
            .enumerate()
        {
            let chunk_hash = BlobHash::from_content(payload);
            stage_manifest_chunk(
                &mut writes,
                chunked_hash,
                index as u64,
                &KvBlobManifestChunk {
                    chunk_hash: chunk_hash.into_bytes(),
                    chunk_size: payload.len() as u64,
                },
            );
            stage_chunk(
                &mut writes,
                chunk_hash,
                BinaryChunkCodec::Raw,
                payload.len() as u64,
                payload,
            );
        }

        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("CAS test rows should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("CAS test read should open");

        let stats = collect_binary_cas_storage_stats(&read)
            .await
            .expect("stats should collect");
        assert_eq!(
            stats,
            BinaryCasStorageStats {
                manifest_rows: 3,
                empty_blob_rows: 1,
                single_chunk_blob_rows: 1,
                chunked_blob_rows: 1,
                manifest_chunk_rows: 2,
                chunk_presence_rows: 3,
                chunk_rows: 3,
                total_chunk_refs: 3,
                logical_blob_bytes: 14,
            }
        );
    }
}
