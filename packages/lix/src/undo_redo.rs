use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::changelog::CommitId;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};

pub(crate) const UNDO_REDO_MARKER_SCHEMA_KEY: &str = "lix_undo_redo_marker";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum UndoRedoKind {
    Undo,
    Redo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UndoRedoMarker {
    pub(crate) branch_id: String,
    pub(crate) kind: UndoRedoKind,
    pub(crate) target_commit_id: CommitId,
    pub(crate) undo_target_after: Option<CommitId>,
    pub(crate) redo_top_after: Option<CommitId>,
    pub(crate) redo_next: Option<CommitId>,
}

pub(crate) fn marker_stage_row(marker: &UndoRedoMarker) -> TransactionWriteRow {
    TransactionWriteRow {
        row_pk: None,
        schema_key: UNDO_REDO_MARKER_SCHEMA_KEY.into(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "branch_id": marker.branch_id,
            "kind": marker.kind,
            "target_commit_id": marker.target_commit_id,
            "undo_target_after": marker.undo_target_after,
            "redo_top_after": marker.redo_top_after,
            "redo_next": marker.redo_next,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: marker.branch_id.clone().into(),
    }
}
