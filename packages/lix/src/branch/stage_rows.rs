use serde_json::json;

use crate::GLOBAL_BRANCH_ID;
use crate::changelog::CommitId;
use crate::row_pk::RowPk;
use crate::transaction_types::{TransactionJson, TransactionWriteRow};

pub(crate) const BRANCH_DESCRIPTOR_SCHEMA_KEY: &str = "lix_branch_descriptor";
pub(crate) const BRANCH_REF_SCHEMA_KEY: &str = "lix_branch_ref";

pub(crate) fn branch_descriptor_stage_row(
    branch_id: &str,
    name: &str,
    hidden: bool,
) -> TransactionWriteRow {
    TransactionWriteRow {
        row_pk: None,
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

pub(crate) fn branch_ref_stage_row(branch_id: &str, commit_id: &CommitId) -> TransactionWriteRow {
    TransactionWriteRow {
        row_pk: None,
        schema_key: BRANCH_REF_SCHEMA_KEY.into(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "id": branch_id,
            "commit_id": commit_id.to_string(),
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: true,
        branch_id: GLOBAL_BRANCH_ID.into(),
    }
}

pub(crate) fn branch_descriptor_tombstone_row(branch_id: &str) -> TransactionWriteRow {
    let mut row = branch_descriptor_stage_row(branch_id, "", false);
    row.row_pk = Some(
        RowPk::uuid_from_canonical(branch_id)
            .expect("branch tombstones target validated UUID identities"),
    );
    row.snapshot = None;
    row
}

pub(crate) fn branch_ref_tombstone_row(branch_id: &str) -> TransactionWriteRow {
    TransactionWriteRow {
        row_pk: Some(
            RowPk::uuid_from_canonical(branch_id)
                .expect("branch tombstones target validated UUID identities"),
        ),
        schema_key: BRANCH_REF_SCHEMA_KEY.into(),
        file_id: None,
        snapshot: None,
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: true,
        branch_id: GLOBAL_BRANCH_ID.into(),
    }
}
