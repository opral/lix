mod create_checkpoint;
mod last_checkpoint;

pub(crate) const CHECKPOINT_LABEL_ID: &str = "lix_label_checkpoint";
pub(crate) const CHECKPOINT_LABEL_NAME: &str = "checkpoint";

pub(crate) fn checkpoint_commit_label_entity_id(commit_id: &str) -> String {
    format!("{commit_id}~lix_commit~lix~{CHECKPOINT_LABEL_ID}")
}

pub(crate) fn checkpoint_commit_label_snapshot(commit_id: &str) -> String {
    serde_json::json!({
        "entity_id": commit_id,
        "schema_key": "lix_commit",
        "file_id": "lix",
        "label_id": CHECKPOINT_LABEL_ID,
    })
    .to_string()
}

pub use create_checkpoint::CreateCheckpointResult;
pub(crate) use create_checkpoint::create_checkpoint_in_session;
pub(crate) use last_checkpoint::apply_public_version_last_checkpoint_side_effects;
