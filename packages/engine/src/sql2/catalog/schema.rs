#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicColumn {
    pub(crate) name: String,
    pub(crate) role: PublicColumnRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicColumnRole {
    Public,
    Hidden,
    Internal,
}
