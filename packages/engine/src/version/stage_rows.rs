use serde_json::json;

use crate::entity_pk::EntityPk;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::GLOBAL_VERSION_ID;

pub(crate) const VERSION_DESCRIPTOR_SCHEMA_KEY: &str = "lix_version_descriptor";
pub(crate) const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";

pub(crate) fn version_descriptor_stage_row(
    version_id: &str,
    name: &str,
    hidden: bool,
) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: Some(EntityPk::single(version_id)),
        schema_key: VERSION_DESCRIPTOR_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "id": version_id,
            "name": name,
            "hidden": hidden,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: false,
        version_id: GLOBAL_VERSION_ID.to_string(),
    }
}

pub(crate) fn version_ref_stage_row(version_id: &str, commit_id: &str) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: Some(EntityPk::single(version_id)),
        schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "id": version_id,
            "commit_id": commit_id,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    }
}

pub(crate) fn version_descriptor_tombstone_row(version_id: &str) -> TransactionWriteRow {
    let mut row = version_descriptor_stage_row(version_id, "", false);
    row.snapshot = None;
    row
}

pub(crate) fn version_ref_tombstone_row(version_id: &str) -> TransactionWriteRow {
    let mut row = version_ref_stage_row(version_id, "");
    row.snapshot = None;
    row
}
