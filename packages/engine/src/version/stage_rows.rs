use serde_json::json;

use crate::entity_identity::EntityIdentity;
use crate::transaction::types::StageRow;
use crate::GLOBAL_VERSION_ID;

pub(crate) const VERSION_DESCRIPTOR_SCHEMA_KEY: &str = "lix_version_descriptor";
pub(crate) const VERSION_DESCRIPTOR_SCHEMA_VERSION: &str = "1";
pub(crate) const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
pub(crate) const VERSION_REF_SCHEMA_VERSION: &str = "1";

pub(crate) fn version_descriptor_stage_row(version_id: &str, name: &str, hidden: bool) -> StageRow {
    StageRow {
        entity_id: Some(EntityIdentity::single(version_id)),
        schema_key: VERSION_DESCRIPTOR_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(encode_snapshot(json!({
            "id": version_id,
            "name": name,
            "hidden": hidden,
        }))),
        metadata: None,
        origin: None,
        schema_version: VERSION_DESCRIPTOR_SCHEMA_VERSION.to_string(),
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: false,
        version_id: GLOBAL_VERSION_ID.to_string(),
    }
}

pub(crate) fn version_ref_stage_row(version_id: &str, commit_id: &str) -> StageRow {
    StageRow {
        entity_id: Some(EntityIdentity::single(version_id)),
        schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(encode_snapshot(json!({
            "id": version_id,
            "commit_id": commit_id,
        }))),
        metadata: None,
        origin: None,
        schema_version: VERSION_REF_SCHEMA_VERSION.to_string(),
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    }
}

pub(crate) fn version_descriptor_tombstone_row(version_id: &str) -> StageRow {
    let mut row = version_descriptor_stage_row(version_id, "", false);
    row.snapshot_content = None;
    row
}

pub(crate) fn version_ref_tombstone_row(version_id: &str) -> StageRow {
    let mut row = version_ref_stage_row(version_id, "");
    row.snapshot_content = None;
    row
}

fn encode_snapshot(value: serde_json::Value) -> String {
    serde_json::to_string(&value).expect("version snapshot should be serializable")
}
