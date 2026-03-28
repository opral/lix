//! Checkpoint subsystem boundary.
//!
//! `checkpoint` is a derived subsystem layered on canonical history. It owns
//! checkpoint-specific acceleration and policy, such as checkpoint labels and
//! last-checkpoint helpers.
//!
//! `checkpoint` is not canonical truth by default. It should remain rebuildable
//! from canonical facts unless the engine deliberately promotes some checkpoint
//! state into canonical ownership later.
//!
mod create_checkpoint;
mod init;
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

pub(crate) use create_checkpoint::create_checkpoint_in_session;
pub use create_checkpoint::CreateCheckpointResult;
pub(crate) use init::{init, seed_bootstrap};
pub(crate) use last_checkpoint::apply_public_version_last_checkpoint_side_effects;
