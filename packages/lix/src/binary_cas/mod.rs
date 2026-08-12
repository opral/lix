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
    BinaryCasManifest, StorageBinaryCasDeltaBaseLayout, decode_binary_cas_chunk,
    decode_binary_cas_manifest, decode_binary_cas_manifest_chunk,
};
pub(crate) use context::{BinaryCasContext, BlobDataReader};
pub(crate) use kv::load_bytes_many;
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

/// Stages the publisher half of the binary-CAS publication fence.
///
/// Every logical CAS publisher — an ordinary commit's root publication, a
/// resumable upload part, a completed-upload receipt — calls this in the same
/// atomic write set that stages its payload rows.
///
/// The fence is deliberately asymmetric, because the two directions it has to
/// stop are not symmetric:
///
/// * A publisher may reuse an immutable payload row instead of restaging it, so
///   it must not commit if a sweep deleted that row after the publisher's
///   planning snapshot. That is enforced here: the publisher asserts the
///   reclamation token is unchanged.
/// * A sweep computes reachability from a snapshot, so it must not commit if a
///   publication rooted new bytes after that snapshot. That is enforced by
///   [`stage_cas_reclamation_fence`]: the sweep asserts the publication token is
///   unchanged, and every publisher rewrites it.
///
/// Publishers, by contrast, never invalidate each other. Payload rows are
/// content-addressed puts with no read-modify-write aggregate, so two
/// publications planned from one snapshot are independent and both may commit.
/// A publisher therefore holds no compare-and-set on the row it writes; making
/// it do so is what turned unrelated concurrent writers — a project-file save
/// alongside media ingest, or two parts of one resumable upload — into
/// `LIX_TRANSACTION_CONFLICT`.
pub(crate) async fn stage_cas_publication_fence(
    store: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    writes: &mut crate::storage_adapter::StorageWriteSet,
    preconditions: &mut Vec<crate::storage_adapter::StoragePrecondition>,
) -> Result<(), crate::LixError> {
    kv::stage_publication_fence(store, writes, preconditions).await
}

/// Stages the sweep half of the binary-CAS publication fence.
///
/// A sweep rotates the reclamation token under a compare-and-set (so two sweeps
/// planned from one snapshot cannot both commit) and asserts the publication
/// token is unchanged (so no publication slipped in after its reachability
/// plan). See [`stage_cas_publication_fence`] for the full argument.
pub(crate) async fn stage_cas_reclamation_fence(
    store: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    writes: &mut crate::storage_adapter::StorageWriteSet,
    preconditions: &mut Vec<crate::storage_adapter::StoragePrecondition>,
) -> Result<(), crate::LixError> {
    kv::stage_reclamation_fence(store, writes, preconditions).await
}
