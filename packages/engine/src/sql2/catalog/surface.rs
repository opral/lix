use super::{PublicColumn, SurfaceCapabilities};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicSurfaceContract {
    pub(crate) name: String,
    pub(crate) kind: PublicSurfaceKind,
    pub(crate) columns: Vec<PublicColumn>,
    pub(crate) capabilities: SurfaceCapabilities,
}

impl PublicSurfaceContract {
    pub(crate) fn public_column(&self, column_name: &str) -> Option<&PublicColumn> {
        self.columns
            .iter()
            .find(|column| column.name == column_name && column.is_public())
    }

    pub(crate) fn column(&self, column_name: &str) -> Option<&PublicColumn> {
        self.columns
            .iter()
            .find(|column| column.name == column_name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicSurfaceKind {
    LixState,
    LixStateByVersion,
    EntityBase { schema_key: String },
    EntityByVersion { schema_key: String },
    EntityHistory { schema_key: String },
    File,
    FileByVersion,
    FileHistory,
    Directory,
    DirectoryByVersion,
    DirectoryHistory,
    Version,
    Change,
    History,
}
