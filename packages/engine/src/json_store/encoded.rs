use crate::json_store::types::JsonRef;
use std::borrow::Cow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonCodec {
    Raw,
    Zstd,
}

pub(crate) struct EncodedJson<'a> {
    pub(crate) json_ref: JsonRef,
    pub(crate) codec: JsonCodec,
    pub(crate) uncompressed_len: usize,
    pub(crate) data: Cow<'a, [u8]>,
}
