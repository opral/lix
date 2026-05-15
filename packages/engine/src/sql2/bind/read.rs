#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundRead {
    pub(crate) source: BoundReadSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundReadSource {
    DataFusion,
}
