mod chunking;
mod codec;
mod context;
pub(crate) mod kv;
pub(crate) mod metrics;
#[cfg(test)]
mod stats;
mod types;

use std::collections::BTreeSet;

pub(crate) use chunking::BinaryCasChunking;
#[cfg(all(feature = "storage-benches", test))]
pub(crate) use codec::encode_binary_cas_manifest;
#[cfg(feature = "storage-benches")]
pub(crate) use codec::{
    BinaryCasManifest, StorageBinaryCasDeltaBaseLayout, decode_binary_cas_manifest,
    decode_binary_cas_manifest_chunk,
};
pub(crate) use context::{BinaryCasContext, BlobDataReader};
pub(crate) use kv::load_bytes_many;
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
    upload_chunks: &BTreeSet<ChunkHash>,
) -> Result<BinaryCasGcSweep, crate::LixError> {
    kv::stage_reclaim_unreachable_binary_cas(store, writes, blob_roots, upload_chunks).await
}
