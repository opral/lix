use serde_json::json;

use crate::GLOBAL_BRANCH_ID;
use crate::entity_pk::EntityPk;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};

pub(crate) const BRANCH_DESCRIPTOR_SCHEMA_KEY: &str = "lix_branch_descriptor";
pub(crate) const BRANCH_REF_SCHEMA_KEY: &str = "lix_branch_ref";

pub(crate) fn branch_descriptor_stage_row(
    branch_id: &str,
    name: &str,
    hidden: bool,
) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: None,
        schema_key: BRANCH_DESCRIPTOR_SCHEMA_KEY.into(),
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
        branch_id: GLOBAL_BRANCH_ID.into(),
    }
}

pub(crate) fn branch_descriptor_tombstone_row(branch_id: &str) -> TransactionWriteRow {
    let mut row = branch_descriptor_stage_row(branch_id, "", false);
    row.entity_pk = Some(
        EntityPk::uuid_from_canonical(branch_id)
            .expect("branch tombstones target validated UUID identities"),
    );
    row.snapshot = None;
    row
}
