#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct UndoOptions {
    /// Target `lix_version.id`. If omitted, uses the active `version_id`.
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct RedoOptions {
    /// Target `lix_version.id`. If omitted, uses the active `version_id`.
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UndoResult {
    pub version_id: String,
    pub target_commit_id: String,
    pub inverse_commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RedoResult {
    pub version_id: String,
    pub target_commit_id: String,
    pub replay_commit_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UndoRedoOperationKind {
    Undo,
    Redo,
}

impl UndoRedoOperationKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Undo => "undo",
            Self::Redo => "redo",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "undo" => Some(Self::Undo),
            "redo" => Some(Self::Redo),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UndoRedoOperationRecord {
    pub(crate) version_id: String,
    pub(crate) operation_commit_id: String,
    pub(crate) operation_kind: UndoRedoOperationKind,
    pub(crate) target_commit_id: String,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct SemanticUndoRedoStacks {
    pub(crate) undo_stack: Vec<String>,
    pub(crate) redo_stack: Vec<String>,
}
