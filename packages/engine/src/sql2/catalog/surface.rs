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
// The state variants keep the existing adapters compilable until their
// implementation-only cleanup lands after this public contract change.
#[allow(dead_code)]
pub(crate) enum PublicSurfaceKind {
    Schema,
    SchemaDefinition,
    LixState,
    LixStateByBranch,
    EntityBase { schema_key: String },
    EntityByBranch { schema_key: String },
    EntityHistory { schema_key: String },
    File,
    FileByBranch,
    FileHistory,
    Directory,
    DirectoryByBranch,
    DirectoryHistory,
    Branch,
    Change,
    History,
}
