use crate::LixError;
use std::ops::Range;

const MEDIA_CHUNK_BYTES: usize = 1024 * 1024;

fn hash_bytes_to_hex(bytes: &[u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String is infallible");
    }
    encoded
}

fn hash_hex_to_bytes(value: &str, context: &str) -> Result<[u8; 32], LixError> {
    if value.len() != 64 {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("{context} hash must contain 64 hexadecimal characters"),
        ));
    }
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(|_| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("{context} hash contains non-hexadecimal characters"),
            )
        })?;
    }
    Ok(bytes)
}

fn binary_blob_hash_bytes(content: &[u8]) -> [u8; 32] {
    *blake3::hash(content).as_bytes()
}

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
}

impl BlobPayload {
    pub(crate) fn from_bytes(bytes: impl Into<crate::Blob>) -> Self {
        let bytes = bytes.into();
        let hash = (!bytes.is_empty()).then(|| BlobId::from_content(&bytes));
        Self { bytes, hash }
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
    /// The authenticated ForkTree manifest object staged for this receipt.
    /// A semantic BlobId alone cannot authorize a physical object edge.
    pub(crate) manifest_object_id: [u8; 32],
    /// True only when the writer authenticated the exact immutable manifest
    /// already present in the retained read; this is an in-memory proof bit,
    /// not a second persisted authority.
    pub(crate) manifest_was_existing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlobChunkReceipt {
    pub(crate) hash: ChunkHash,
    pub(crate) size_bytes: u64,
    /// The authenticated ForkTree chunk object carrying this content hash.
    /// The physical owner is retained across resumable receipt rows.
    pub(crate) object_id: [u8; 32],
}
