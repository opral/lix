mod chunking;
mod codec;
mod context;
pub(crate) mod kv;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BinaryBlobWrite<'a> {
    pub file_id: &'a str,
    pub version_id: &'a str,
    pub data: &'a [u8],
}

#[cfg(feature = "storage-benches")]
pub(crate) use codec::binary_blob_hash_hex;
pub(crate) use context::{BinaryCasContext, BlobDataReader};
