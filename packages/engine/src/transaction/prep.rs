use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::entity_pk::EntityPk;
use crate::untracked_state::UntrackedStateRow;
use crate::{LixError, GLOBAL_BRANCH_ID};

pub(crate) struct PreparedBranchRefRow {
    pub(crate) row: UntrackedStateRow,
}

pub(crate) fn prepare_branch_ref_row(
    branch_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> Result<PreparedBranchRefRow, LixError> {
    let snapshot = serde_json::json!({
        "id": branch_id,
        "commit_id": commit_id,
    });
    let snapshot = crate::json_store::NormalizedJson::from_value(
        &snapshot,
        "engine branch-ref snapshot_content",
    )?;

    Ok(PreparedBranchRefRow {
        row: UntrackedStateRow {
            entity_pk: EntityPk::single(branch_id),
            schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot.as_str().to_string()),
            metadata: None,
            created_at: timestamp.to_string(),
            updated_at: timestamp.to_string(),
            global: true,
            branch_id: GLOBAL_BRANCH_ID.to_string(),
        },
    })
}
