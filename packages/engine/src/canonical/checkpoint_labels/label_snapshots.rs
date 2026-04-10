pub(crate) const CHECKPOINT_LABEL_ID: &str = "lix_label_checkpoint";
pub(crate) const CHECKPOINT_LABEL_NAME: &str = "checkpoint";
pub(crate) const CHECKPOINT_LABEL_SCHEMA_KEY: &str = "lix_label";
pub(crate) const CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY: &str = "lix_entity_label";

pub(crate) fn checkpoint_label_snapshot() -> String {
    serde_json::json!({
        "id": CHECKPOINT_LABEL_ID,
        "name": CHECKPOINT_LABEL_NAME,
    })
    .to_string()
}

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
