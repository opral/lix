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

pub(crate) use context::{BinaryCasContext, BlobDataReader};
