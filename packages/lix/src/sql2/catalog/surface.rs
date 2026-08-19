use super::{PublicColumn, SurfaceCapabilities};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicSurfaceContract {
    pub(crate) name: String,
    pub(crate) kind: PublicSurfaceKind,
    pub(crate) columns: Vec<PublicColumn>,
    pub(crate) capabilities: SurfaceCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicHistoryContract {
    pub(crate) relation_name: String,
    pub(crate) kind: PublicHistoryKind,
    pub(crate) columns: Vec<PublicColumn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicHistoryKind {
    Schema { schema_key: String },
    File,
    Directory,
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
    SchemaBase { schema_key: String },
    File,
    Directory,
    Branch,
    HistoryFunction,
    WorkingDiff,
    Revert,
    Apply,
    CreateCheckpoint,
    Change,
}
