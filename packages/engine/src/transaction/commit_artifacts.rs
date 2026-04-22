use crate::transaction::WriteBatch;
use crate::LixError;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PendingCommitLane {
    Version(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PendingCommitState {
    pub lane: PendingCommitLane,
    pub commit_id: String,
    pub commit_change_id: String,
    pub commit_change_snapshot_id: String,
    pub commit_snapshot: JsonValue,
}

pub(crate) async fn load_commit_idempotency_replay_in_transaction(
    transaction: &mut dyn crate::LixBackendTransaction,
    write_lane: &str,
    idempotency_kind: &str,
    idempotency_value: &str,
    parent_head_snapshot_content: &str,
) -> Result<Option<String>, LixError> {
    crate::transaction::commit_idempotency::load_commit_idempotency_replay_in_transaction(
        transaction,
        write_lane,
        idempotency_kind,
        idempotency_value,
        parent_head_snapshot_content,
    )
    .await
}

pub(crate) fn append_commit_idempotency_row(
    write_batch: &mut WriteBatch,
    write_lane: &str,
    idempotency_key: &str,
    idempotency_kind: &str,
    idempotency_value: &str,
    parent_head_snapshot_content: &str,
    commit_id: &str,
    created_at: &str,
) {
    crate::transaction::commit_idempotency::append_commit_idempotency_row(
        write_batch,
        write_lane,
        idempotency_key,
        idempotency_kind,
        idempotency_value,
        parent_head_snapshot_content,
        commit_id,
        created_at,
    );
}

pub(crate) async fn load_commit_change_snapshot_id_in_transaction(
    transaction: &mut dyn crate::LixBackendTransaction,
    change_id: &str,
    commit_id: &str,
) -> Result<Option<String>, LixError> {
    crate::transaction::commit_idempotency::load_commit_change_snapshot_id_in_transaction(
        transaction,
        change_id,
        commit_id,
    )
    .await
}
