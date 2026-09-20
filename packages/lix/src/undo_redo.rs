mod navigation;
pub(crate) use navigation::{execute, is_undo_metadata, replayed_change_matches};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::changelog::CommitId;
use crate::transaction_types::{TransactionJson, TransactionWriteRow};

pub(crate) const UNDO_STATE_SCHEMA_KEY: &str = "lix_undo_state";

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
    #[serde(default)]
    pub(crate) effects: Vec<String>,
    #[serde(default)]
    pub(crate) selected_files: std::collections::BTreeSet<String>,
    #[serde(default)]
    pub(crate) checkpoint: bool,
    #[serde(default)]
    pub(crate) source_undo_commit_id: Option<CommitId>,
    pub(crate) baseline_before: Option<CommitId>,
    pub(crate) baseline_after: Option<CommitId>,
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
            "effects": marker.effects,
            "selected_files": marker.selected_files,
            "checkpoint": marker.checkpoint,
            "source_undo_commit_id": marker.source_undo_commit_id,
            "baseline_before": marker.baseline_before,
            "baseline_after": marker.baseline_after,
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
