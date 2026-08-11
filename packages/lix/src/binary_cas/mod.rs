mod chunking;
mod codec;
mod context;
mod kv;
pub(crate) mod metrics;
#[cfg(test)]
mod stats;
mod types;

use std::collections::{BTreeMap, BTreeSet};

#[cfg(all(feature = "storage-benches", test))]
pub(crate) use codec::encode_binary_cas_manifest;
#[cfg(feature = "storage-benches")]
pub(crate) use codec::{
    BinaryCasManifest, StorageBinaryCasDeltaBaseLayout, decode_binary_cas_manifest,
    decode_binary_cas_manifest_chunk,
};
pub(crate) use context::{BinaryCasContext, BlobDataReader};
pub(crate) use kv::load_bytes_many;
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) use kv::{
    BINARY_CAS_CHUNK_PRESENCE_SPACE, BINARY_CAS_CHUNK_SPACE, BINARY_CAS_MANIFEST_CHUNK_SPACE,
    BINARY_CAS_MANIFEST_SPACE,
};
pub(crate) use types::{
    BlobBytesBatch, BlobChunkReceipt, BlobDeltaBaseLayout, BlobDeltaSegment, BlobEditSplice,
    BlobId, BlobLayout, BlobMetadata, BlobMetadataBatch, BlobPayload, BlobRangeBytes,
    BlobRangeBytesBatch, BlobSameLengthSplice, BlobWriteReceipt, ChunkHash,
};

/// Summary of one authenticated, offline binary-CAS sweep.
///
/// This is rebuildable maintenance state. It is never consulted by serving
/// reads and is not a refcount or a second payload authority.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BinaryCasGcSweep {
    pub(crate) live_blob_count: usize,
    pub(crate) live_chunk_count: usize,
    pub(crate) reclaimed_manifest_rows: usize,
    pub(crate) reclaimed_manifest_chunk_rows: usize,
    pub(crate) reclaimed_chunk_rows: usize,
    pub(crate) reclaimed_chunk_bytes: u64,
}

/// Stages the owner's authenticated binary-CAS reclamation operation.
pub(crate) async fn stage_gc_reclamation(
    store: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    writes: &mut crate::storage_adapter::StorageWriteSet,
    blob_roots: &BTreeSet<BlobId>,
    upload_chunks: &BTreeMap<ChunkHash, u64>,
) -> Result<BinaryCasGcSweep, crate::LixError> {
    kv::stage_reclaim_unreachable_binary_cas(store, writes, blob_roots, upload_chunks).await
}

/// Stages the single authenticated publication/reclamation epoch owned by the
/// binary CAS. Every logical CAS publisher and every sweep rotates this row in
/// the same atomic commit. A publisher and sweep planned from one snapshot can
/// therefore never both commit, including when publication reuses every
/// immutable payload row.
pub(crate) async fn stage_mutation_epoch(
    store: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    writes: &mut crate::storage_adapter::StorageWriteSet,
    preconditions: &mut Vec<crate::storage_adapter::StoragePrecondition>,
) -> Result<(), crate::LixError> {
    let (current, token) = kv::load_mutation_epoch(store).await?;
    kv::stage_mutation_epoch(writes, preconditions, current, token)
}
