#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonCodec {
    Raw,
    Zstd,
}
