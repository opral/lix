use super::{PublicColumn, SurfaceCapabilities};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicSurfaceContract {
    pub(crate) name: String,
    pub(crate) kind: PublicSurfaceKind,
    pub(crate) columns: Vec<PublicColumn>,
    pub(crate) capabilities: SurfaceCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicSurfaceKind {
    LixState,
    EntityBase { schema_key: String },
    EntityByVersion { schema_key: String },
    File,
    FileHistory,
    Directory,
    DirectoryHistory,
    Version,
    Change,
    History,
}
