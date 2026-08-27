use super::{PublicColumn, SurfaceCapabilities};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicSurfaceContract {
    pub(crate) name: String,
    pub(crate) class: PublicSurfaceClass,
    pub(crate) kind: PublicSurfaceKind,
    pub(crate) columns: Vec<PublicColumn>,
    pub(crate) capabilities: SurfaceCapabilities,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicRelationKind {
    Base,
    View,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicSurfaceClass {
    Relation(PublicRelationKind),
    TableFunction,
    CommandSink,
    ScalarFunction,
}

impl PublicSurfaceClass {
    pub(crate) fn sql_name(self) -> &'static str {
        match self {
            Self::Relation(_) => "RELATION",
            Self::TableFunction => "TABLE_FUNCTION",
            Self::CommandSink => "COMMAND_SINK",
            Self::ScalarFunction => "SCALAR_FUNCTION",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicScalarFunctionContract {
    pub(crate) name: String,
    pub(crate) class: PublicSurfaceClass,
}

pub(crate) const PUBLIC_SCALAR_FUNCTION_NAMES: [&str; 7] = [
    "lix_active_account_id",
    "lix_active_branch_commit_id",
    "lix_active_branch_id",
    "lix_latest_checkpoint_commit_id",
    "lix_root_commit_id",
    "lix_row_ref",
    "uuidv7",
];

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
    DiffFunction,
    CheckpointFunction,
    StateAtFunction,
    CommitAncestryFunction,
    Revert,
    Apply,
    Restore,
    Change,
}

impl PublicSurfaceKind {
    pub(crate) fn accepts_class(&self, class: PublicSurfaceClass) -> bool {
        match self {
            Self::SchemaBase { .. }
            | Self::File
            | Self::Directory
            | Self::Branch
            | Self::Change => matches!(class, PublicSurfaceClass::Relation(_)),
            Self::HistoryFunction
            | Self::DiffFunction
            | Self::CheckpointFunction
            | Self::StateAtFunction
            | Self::CommitAncestryFunction => class == PublicSurfaceClass::TableFunction,
            Self::Revert | Self::Apply | Self::Restore => {
                class == PublicSurfaceClass::CommandSink
            }
        }
    }
}
