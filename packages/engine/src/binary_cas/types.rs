use crate::LixError;
use crate::binary_cas::codec::BinaryChunkCodec;
use crate::binary_cas::codec::{binary_blob_hash_bytes, hash_bytes_to_hex, hash_hex_to_bytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct BlobHash([u8; 32]);

impl BlobHash {
    pub(crate) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn from_content(content: &[u8]) -> Self {
        Self(binary_blob_hash_bytes(content))
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

/// A host-verified fixed-width replacement in an already materialized blob.
///
/// The ordinary SQL surface still submits complete replacement bytes. The
/// transaction layer creates this internal hint only after it has verified a
/// v2 file transition against the exact accepted document. A CAS writer may
/// then retain the base blob's chunk boundaries and references for every
/// non-overlapping chunk instead of rechunking the complete replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlobSameLengthSplice {
    pub(crate) base_blob_hash: BlobHash,
    pub(crate) offset: usize,
    pub(crate) length: usize,
}

impl BlobSameLengthSplice {
    pub(crate) fn new(base_blob_hash: BlobHash, offset: usize, length: usize) -> Self {
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
    hash: Option<BlobHash>,
}

impl BlobPayload {
    pub(crate) fn from_bytes(bytes: impl Into<crate::Blob>) -> Self {
        let bytes = bytes.into();
        let hash = (!bytes.is_empty()).then(|| BlobHash::from_content(&bytes));
        Self { bytes, hash }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn shared_bytes(&self) -> crate::Blob {
        self.bytes.clone()
    }

    pub(crate) fn hash(&self) -> Option<BlobHash> {
        self.hash
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlobLayout {
    Empty,
    SingleChunk { chunk_hash: BlobHash },
    Chunked { chunk_count: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobMetadata {
    pub(crate) hash: BlobHash,
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
    pub(crate) hash: BlobHash,
    pub(crate) size_bytes: u64,
    pub(crate) layout: BlobLayout,
}

#[derive(musli::Decode)]
#[musli(packed)]
pub(crate) struct BinaryCasChunkView<'a> {
    pub(crate) codec: BinaryChunkCodec,
    pub(crate) uncompressed_len: u64,
    #[musli(bytes)]
    pub(crate) payload: &'a [u8],
}
