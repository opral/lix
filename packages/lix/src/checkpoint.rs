use serde_json::json;

use crate::transaction::types::{TransactionJson, TransactionWriteRow};

/// Schema key for the authenticated state marker written by a checkpoint
/// transaction. Chronology reads live under ForkTree; this module retains
/// only the transaction's semantic marker row construction.
pub(crate) const CHECKPOINT_MARKER_SCHEMA_KEY: &str = "lix_checkpoint_marker";

pub(crate) fn checkpoint_marker_stage_row(branch_id: &str) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: None,
        schema_key: CHECKPOINT_MARKER_SCHEMA_KEY.into(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "branch_id": branch_id,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: branch_id.into(),
    }
}
