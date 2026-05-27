use serde_json::json;

use crate::entity_pk::EntityPk;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::GLOBAL_BRANCH_ID;

pub(crate) const BRANCH_DESCRIPTOR_SCHEMA_KEY: &str = "lix_branch_descriptor";
pub(crate) const BRANCH_REF_SCHEMA_KEY: &str = "lix_branch_ref";

pub(crate) fn branch_descriptor_stage_row(
    branch_id: &str,
    name: &str,
    hidden: bool,
) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: Some(EntityPk::single(branch_id)),
        schema_key: BRANCH_DESCRIPTOR_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "id": branch_id,
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
        branch_id: GLOBAL_BRANCH_ID.to_string(),
    }
}

pub(crate) fn branch_ref_stage_row(branch_id: &str, commit_id: &str) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: Some(EntityPk::single(branch_id)),
        schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "id": branch_id,
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
        branch_id: GLOBAL_BRANCH_ID.to_string(),
    }
}

pub(crate) fn branch_descriptor_tombstone_row(branch_id: &str) -> TransactionWriteRow {
    let mut row = branch_descriptor_stage_row(branch_id, "", false);
    row.snapshot = None;
    row
}

pub(crate) fn branch_ref_tombstone_row(branch_id: &str) -> TransactionWriteRow {
    let mut row = branch_ref_stage_row(branch_id, "");
    row.snapshot = None;
    row
}
