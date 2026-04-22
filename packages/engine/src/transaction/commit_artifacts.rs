use crate::transaction::WriteBatch;
use crate::{LixBackendTransaction, LixError, Value};
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
    transaction: &mut dyn LixBackendTransaction,
    write_lane: &str,
    idempotency_kind: &str,
    idempotency_value: &str,
    parent_head_snapshot_content: &str,
) -> Result<Option<String>, LixError> {
    let result = transaction
        .execute(
            "SELECT commit_id \
             FROM lix_internal_commit_idempotency \
             WHERE write_lane = $1 \
               AND idempotency_kind = $2 \
               AND idempotency_value = $3 \
               AND parent_head_snapshot_content = $4 \
             LIMIT 1",
            &[
                Value::Text(write_lane.to_string()),
                Value::Text(idempotency_kind.to_string()),
                Value::Text(idempotency_value.to_string()),
                Value::Text(parent_head_snapshot_content.to_string()),
            ],
        )
        .await?;
    Ok(result.rows.first().and_then(|row| match row.first() {
        Some(Value::Text(commit_id)) if !commit_id.is_empty() => Some(commit_id.clone()),
        _ => None,
    }))
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
    write_batch.push_statement(
        "INSERT INTO lix_internal_commit_idempotency \
         (write_lane, idempotency_key, idempotency_kind, idempotency_value, parent_head_snapshot_content, commit_id, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        vec![
            Value::Text(write_lane.to_string()),
            Value::Text(idempotency_key.to_string()),
            Value::Text(idempotency_kind.to_string()),
            Value::Text(idempotency_value.to_string()),
            Value::Text(parent_head_snapshot_content.to_string()),
            Value::Text(commit_id.to_string()),
            Value::Text(created_at.to_string()),
        ],
    );
}

pub(crate) async fn load_commit_change_snapshot_id_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    change_id: &str,
    commit_id: &str,
) -> Result<Option<String>, LixError> {
    let result = transaction
        .execute(
            "SELECT snapshot_id \
             FROM lix_internal_change \
             WHERE id = $1 \
               AND schema_key = 'lix_commit' \
               AND entity_id = $2 \
             LIMIT 1",
            &[
                Value::Text(change_id.to_string()),
                Value::Text(commit_id.to_string()),
            ],
        )
        .await?;
    Ok(result.rows.first().and_then(|row| match row.first() {
        Some(Value::Text(snapshot_id)) if !snapshot_id.is_empty() => Some(snapshot_id.clone()),
        _ => None,
    }))
}
