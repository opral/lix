use crate::json_store::types::JsonRef;
use std::borrow::Cow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JsonCodec {
    Raw,
    Zstd,
}

pub(super) struct EncodedJson<'a> {
    pub(super) json_ref: JsonRef,
    pub(super) codec: JsonCodec,
    pub(super) uncompressed_len: usize,
    pub(super) data: Cow<'a, [u8]>,
}
