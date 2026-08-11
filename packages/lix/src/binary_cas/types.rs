use crate::LixError;
use crate::binary_cas::chunking::MEDIA_CHUNK_BYTES;
use crate::binary_cas::codec::BinaryChunkCodec;
use crate::binary_cas::codec::{binary_blob_hash_bytes, hash_bytes_to_hex, hash_hex_to_bytes};
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct BlobId([u8; 32]);

impl BlobId {
    pub(crate) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn from_content(content: &[u8]) -> Self {
        if content.len() <= MEDIA_CHUNK_BYTES {
            return Self::from_single_chunk(ChunkHash::from_content(content));
        }
        let chunks = content
            .chunks(MEDIA_CHUNK_BYTES)
            .map(|chunk| (ChunkHash::from_content(chunk), chunk.len() as u64));
        Self::from_chunks(content.len() as u64, chunks)
    }

    pub(crate) fn from_single_chunk(chunk_hash: ChunkHash) -> Self {
        Self(chunk_hash.into_bytes())
    }

    /// Computes the canonical identity of a fixed-chunk blob without opening
    /// its payload. Upload recovery persists precisely these bounded receipts.
    pub(crate) fn from_chunks(
        size_bytes: u64,
        chunks: impl IntoIterator<Item = (ChunkHash, u64)>,
    ) -> Self {
        let mut hasher = blake3::Hasher::new_derive_key("lix binary cas fixed manifest v3");
        hasher.update(&size_bytes.to_le_bytes());
        for (hash, size) in chunks {
            hasher.update(&size.to_le_bytes());
            hasher.update(hash.as_bytes());
        }
        Self(*hasher.finalize().as_bytes())
    }

    pub(crate) fn from_hex(hash_hex: &str) -> Result<Self, LixError> {
        Ok(Self(hash_hex_to_bytes(hash_hex, "binary CAS blob")?))
    }

    pub(crate) fn to_hex(self) -> String {
        hash_bytes_to_hex(&self.0)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// The raw content hash of one immutable CAS chunk.
///
/// A `ChunkHash` can equal a small blob's `BlobId` byte-for-byte, but it is
/// deliberately a distinct type: chunk keys must never be accepted where a
/// manifest identity is required.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ChunkHash([u8; 32]);

impl ChunkHash {
    pub(crate) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn from_content(content: &[u8]) -> Self {
        Self(binary_blob_hash_bytes(content))
    }

    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    pub(crate) fn to_hex(self) -> String {
        hash_bytes_to_hex(&self.0)
    }
}

/// A host-verified fixed-width replacement in an already materialized blob.
///
/// The ordinary SQL surface still submits complete replacement bytes. The
/// transaction layer creates this internal hint only after it has verified a
/// v2 file transition against the exact accepted document. A CAS writer may
/// then retain the base blob's chunk boundaries and references for every
/// non-overlapping chunk instead of rechunking the complete replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlobSameLengthSplice {
    pub(crate) base_blob_hash: BlobId,
    pub(crate) offset: usize,
    pub(crate) length: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlobEditSplice {
    pub(crate) base_blob_hash: BlobId,
    pub(crate) offset: usize,
    pub(crate) delete_len: usize,
    pub(crate) insert_len: usize,
}

impl BlobSameLengthSplice {
    pub(crate) fn new(base_blob_hash: BlobId, offset: usize, length: usize) -> Self {
        Self {
            base_blob_hash,
            offset,
            length,
        }
    }

    pub(crate) fn end(self) -> Option<usize> {
        self.offset.checked_add(self.length)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobPayload {
    bytes: crate::Blob,
    hash: Option<BlobId>,
    chunks: Arc<[BlobChunkReceipt]>,
}

impl BlobPayload {
    pub(crate) fn from_bytes(bytes: impl Into<crate::Blob>) -> Self {
        let bytes = bytes.into();
        let chunks = bytes
            .chunks(MEDIA_CHUNK_BYTES)
            .map(|chunk| BlobChunkReceipt {
                hash: ChunkHash::from_content(chunk),
                size_bytes: chunk.len() as u64,
            })
            .collect::<Arc<[_]>>();
        let hash = match chunks.as_ref() {
            [] => None,
            [chunk] => Some(BlobId::from_single_chunk(chunk.hash)),
            chunks => Some(BlobId::from_chunks(
                bytes.len() as u64,
                chunks.iter().map(|chunk| (chunk.hash, chunk.size_bytes)),
            )),
        };
        Self {
            bytes,
            hash,
            chunks,
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn shared_bytes(&self) -> crate::Blob {
        self.bytes.clone()
    }

    pub(crate) fn hash(&self) -> Option<BlobId> {
        self.hash
    }

    /// Canonical fixed-chunk identities derived from these exact immutable
    /// bytes. Keeping the receipts inside the payload prevents CAS staging
    /// from hashing every large-file chunk a second time.
    pub(crate) fn chunks(&self) -> &[BlobChunkReceipt] {
        &self.chunks
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlobDeltaSegment {
    Copy { offset: u64, length: u64 },
    Insert { bytes: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlobDeltaBaseLayout {
    SingleChunk { chunk_hash: ChunkHash },
    Chunked { chunk_count: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlobLayout {
    Empty,
    SingleChunk {
        chunk_hash: ChunkHash,
    },
    Chunked {
        chunk_count: u32,
    },
    /// One-level, flattened copy/insert program against a canonical full blob,
    /// so reads never walk a history chain.
    Delta {
        base_blob_hash: BlobId,
        base_size_bytes: u64,
        base_layout: BlobDeltaBaseLayout,
        segments: Vec<BlobDeltaSegment>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobMetadata {
    pub(crate) hash: BlobId,
    pub(crate) size_bytes: u64,
    pub(crate) layout: BlobLayout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobMetadataBatch {
    entries: Vec<Option<BlobMetadata>>,
}

impl BlobMetadataBatch {
    pub(crate) fn new(entries: Vec<Option<BlobMetadata>>) -> Self {
        Self { entries }
    }

    pub(crate) fn into_vec(self) -> Vec<Option<BlobMetadata>> {
        self.entries
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobBytesBatch {
    entries: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobRangeBytes {
    pub(crate) bytes: Vec<u8>,
    pub(crate) total_size: u64,
    pub(crate) range: Range<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobRangeBytesBatch {
    entries: Vec<Option<BlobRangeBytes>>,
}

impl BlobRangeBytesBatch {
    pub(crate) fn new(entries: Vec<Option<BlobRangeBytes>>) -> Self {
        Self { entries }
    }

    pub(crate) fn into_vec(self) -> Vec<Option<BlobRangeBytes>> {
        self.entries
    }
}

impl BlobBytesBatch {
    pub(crate) fn new(entries: Vec<Option<Vec<u8>>>) -> Self {
        Self { entries }
    }

    pub(crate) fn into_vec(self) -> Vec<Option<Vec<u8>>> {
        self.entries
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobWriteReceipt {
    pub(crate) hash: BlobId,
    pub(crate) size_bytes: u64,
    pub(crate) layout: BlobLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlobChunkReceipt {
    pub(crate) hash: ChunkHash,
    pub(crate) size_bytes: u64,
}

#[derive(musli::Decode)]
#[musli(packed)]
pub(crate) struct BinaryCasChunkView<'a> {
    pub(crate) codec: BinaryChunkCodec,
    pub(crate) uncompressed_len: u64,
    #[musli(bytes)]
    pub(crate) payload: &'a [u8],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_carries_exact_canonical_chunk_receipts() {
        let bytes = (0..MEDIA_CHUNK_BYTES * 2 + 17)
            .map(|index| (index as u8).wrapping_mul(31))
            .collect::<Vec<_>>();
        let payload = BlobPayload::from_bytes(bytes.clone());

        assert_eq!(payload.chunks().len(), 3);
        assert_eq!(payload.chunks()[0].size_bytes, MEDIA_CHUNK_BYTES as u64);
        assert_eq!(payload.chunks()[1].size_bytes, MEDIA_CHUNK_BYTES as u64);
        assert_eq!(payload.chunks()[2].size_bytes, 17);
        assert_eq!(payload.hash(), Some(BlobId::from_content(&bytes)));
        for (receipt, chunk) in payload.chunks().iter().zip(bytes.chunks(MEDIA_CHUNK_BYTES)) {
            assert_eq!(receipt.hash, ChunkHash::from_content(chunk));
            assert_eq!(receipt.size_bytes, chunk.len() as u64);
        }

        let clone = payload.clone();
        assert!(std::ptr::eq(
            payload.chunks().as_ptr(),
            clone.chunks().as_ptr()
        ));
        assert_eq!(payload.bytes(), clone.bytes());
    }
}
