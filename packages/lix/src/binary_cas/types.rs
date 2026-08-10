use crate::LixError;
use std::ops::Range;

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

    /// Derives the sole semantic identity from the canonical authenticated
    /// Merkle layout. This name deliberately excludes the retired flat/fixed
    /// manifest constructor contract.
    pub(crate) fn from_canonical_content(content: &[u8]) -> Self {
        crate::forktree::canonical_blob_id_for_content(content)
            .expect("in-memory content has canonical Merkle geometry")
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
    pub(crate) fn from_content(content: &[u8]) -> Self {
        Self(binary_blob_hash_bytes(content))
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
        let hash = (!bytes.is_empty()).then(|| BlobId::from_canonical_content(&bytes));
        Self { bytes, hash }
    }

    pub(crate) fn from_bytes_with_hash(bytes: impl Into<crate::Blob>, hash: BlobId) -> Self {
        let bytes = bytes.into();
        let hash = (!bytes.is_empty()).then_some(hash);
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
pub(crate) enum BlobLayout {
    Empty,
    SingleChunk { chunk_hash: ChunkHash },
    Chunked { chunk_count: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BlobBytesEntry {
    Owned(Vec<u8>),
    Shared(bytes::Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobBytesBatch {
    entries: Vec<Option<BlobBytesEntry>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobRangeBytes {
    pub(crate) bytes: bytes::Bytes,
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
        Self {
            entries: entries
                .into_iter()
                .map(|entry| entry.map(BlobBytesEntry::Owned))
                .collect(),
        }
    }

    /// Retains already shared payload buffers without copying. The caller is
    /// responsible for having authenticated every buffer before construction.
    pub(crate) fn from_shared(entries: Vec<Option<bytes::Bytes>>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|entry| entry.map(BlobBytesEntry::Shared))
                .collect(),
        }
    }

    /// Retains shared `Blob` payloads without copying. The caller is
    /// responsible for having authenticated every buffer before construction.
    pub(crate) fn from_blobs(entries: Vec<Option<crate::Blob>>) -> Self {
        Self::from_shared(
            entries
                .into_iter()
                .map(|entry| entry.map(crate::Blob::into_bytes))
                .collect(),
        )
    }

    pub(crate) fn into_vec(self) -> Vec<Option<Vec<u8>>> {
        self.entries
            .into_iter()
            .map(|entry| {
                entry.map(|entry| match entry {
                    BlobBytesEntry::Owned(bytes) => bytes,
                    BlobBytesEntry::Shared(bytes) => match bytes.try_into_mut() {
                        Ok(bytes) => bytes.into(),
                        Err(bytes) => bytes.to_vec(),
                    },
                })
            })
            .collect()
    }

    pub(crate) fn into_shared_vec(self) -> Vec<Option<bytes::Bytes>> {
        self.entries
            .into_iter()
            .map(|entry| {
                entry.map(|entry| match entry {
                    BlobBytesEntry::Owned(bytes) => bytes.into(),
                    BlobBytesEntry::Shared(bytes) => bytes,
                })
            })
            .collect()
    }

    pub(crate) fn into_blob_vec(self) -> Vec<Option<crate::Blob>> {
        self.entries
            .into_iter()
            .map(|entry| {
                entry.map(|entry| match entry {
                    BlobBytesEntry::Owned(bytes) => bytes.into(),
                    BlobBytesEntry::Shared(bytes) => bytes.into(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::BlobBytesBatch;
    use bytes::Bytes;

    #[test]
    fn owned_batches_transfer_vec_storage_without_copying() {
        let source = vec![0x5a; 32];
        let source_ptr = source.as_ptr();
        let returned = BlobBytesBatch::new(vec![Some(source)])
            .into_vec()
            .pop()
            .flatten()
            .expect("owned entry");
        assert_eq!(returned.as_ptr(), source_ptr);
    }

    #[test]
    fn shared_batches_transfer_bytes_storage_into_blob_without_copying() {
        let source = Bytes::from_static(b"shared authenticated payload");
        let source_ptr = source.as_ptr();
        let blob = BlobBytesBatch::from_shared(vec![Some(source)])
            .into_blob_vec()
            .pop()
            .flatten()
            .expect("shared entry");
        assert_eq!(blob.as_ref().as_ptr(), source_ptr);
    }

    #[test]
    fn unique_shared_batches_transfer_vec_storage_without_copying() {
        let source = vec![0xa5; 32];
        let source_ptr = source.as_ptr();
        let returned = BlobBytesBatch::from_shared(vec![Some(Bytes::from(source))])
            .into_vec()
            .pop()
            .flatten()
            .expect("unique shared entry");
        assert_eq!(returned.as_ptr(), source_ptr);
    }

    #[test]
    fn shared_slice_and_static_batches_keep_copy_fallback_correct() {
        let backing = Bytes::from_static(b"0123456789");
        let sliced = backing.slice(2..7);
        let values =
            BlobBytesBatch::from_shared(vec![Some(sliced), Some(Bytes::from_static(b"static"))])
                .into_vec();
        assert_eq!(values[0].as_deref(), Some(b"23456".as_slice()));
        assert_eq!(values[1].as_deref(), Some(b"static".as_slice()));
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
