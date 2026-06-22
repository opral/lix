use std::ops::Bound;

use crate::LixError;
use crate::backend::{
    BackendError, BackendRead, CoreProjection, KeyRange, KeyRef, ProjectedValueRef, ScanOptions,
    SpaceId,
};
use crate::binary_cas::codec::{BinaryCasManifest, decode_binary_cas_manifest};
use crate::binary_cas::kv::{
    BINARY_CAS_CHUNK_SPACE, BINARY_CAS_MANIFEST_CHUNK_SPACE, BINARY_CAS_MANIFEST_SPACE,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BinaryCasStorageStats {
    pub manifest_rows: u64,
    pub empty_blob_rows: u64,
    pub single_chunk_blob_rows: u64,
    pub chunked_blob_rows: u64,
    pub manifest_chunk_rows: u64,
    pub chunk_rows: u64,
    pub total_chunk_refs: u64,
    pub logical_blob_bytes: u64,
}

pub(crate) fn collect_binary_cas_storage_stats<R>(
    read: &R,
) -> Result<BinaryCasStorageStats, LixError>
where
    R: BackendRead + ?Sized,
{
    let mut stats = BinaryCasStorageStats::default();
    stats.manifest_rows = scan_space(
        read,
        BINARY_CAS_MANIFEST_SPACE.id,
        CoreProjection::FullValue,
        |value| {
            let ProjectedValueRef::FullValue(bytes) = value else {
                return Err(BackendError::Corruption(
                    "binary CAS manifest scan returned key-only value".to_string(),
                ));
            };
            let manifest = decode_binary_cas_manifest(bytes).map_err(|error| {
                BackendError::Corruption(format!("invalid binary CAS manifest: {error}"))
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
    )?;
    stats.manifest_chunk_rows = count_space(read, BINARY_CAS_MANIFEST_CHUNK_SPACE.id)?;
    stats.chunk_rows = count_space(read, BINARY_CAS_CHUNK_SPACE.id)?;
    Ok(stats)
}

fn count_space<R>(read: &R, space: SpaceId) -> Result<u64, LixError>
where
    R: BackendRead + ?Sized,
{
    scan_space(read, space, CoreProjection::KeyOnly, |_| Ok(()))
}

fn scan_space<R, F>(
    read: &R,
    space: SpaceId,
    projection: CoreProjection,
    mut visit: F,
) -> Result<u64, LixError>
where
    R: BackendRead + ?Sized,
    F: for<'a> FnMut(ProjectedValueRef<'a>) -> Result<(), BackendError>,
{
    let range = KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let mut resume_after = None;
    let mut row_count = 0_u64;

    loop {
        let mut last_key = None;
        let mut emitted = 0_u64;
        let result = read.scan(
            space,
            range.clone(),
            ScanOptions {
                projection,
                limit_rows: 4096,
                resume_after: resume_after.as_ref(),
            },
            &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                emitted += 1;
                last_key = Some(key.to_owned_key());
                visit(value)
            },
        )?;
        row_count += emitted;
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
    use crate::backend::{Backend, InMemoryBackend, ReadOptions, WriteOptions};
    use crate::binary_cas::BlobHash;
    use crate::binary_cas::codec::BinaryChunkCodec;
    use crate::binary_cas::kv::{
        KvBlobManifestChunk, stage_chunk, stage_manifest, stage_manifest_chunk,
    };
    use crate::storage::StorageContext;

    #[test]
    fn counts_binary_cas_storage_rows() {
        let backend = InMemoryBackend::new();
        let storage = StorageContext::new(backend.clone());
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
            .commit_write_set(writes, WriteOptions::default())
            .expect("CAS test rows should commit");
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("CAS test read should open");

        let stats = collect_binary_cas_storage_stats(&read).expect("stats should collect");
        assert_eq!(
            stats,
            BinaryCasStorageStats {
                manifest_rows: 3,
                empty_blob_rows: 1,
                single_chunk_blob_rows: 1,
                chunked_blob_rows: 1,
                manifest_chunk_rows: 2,
                chunk_rows: 3,
                total_chunk_refs: 3,
                logical_blob_bytes: 14,
            }
        );
    }
}
