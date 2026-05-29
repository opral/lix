use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::changelog::CommitId;
use crate::entity_pk::EntityPk;
use crate::untracked_state::UntrackedStateRow;
use crate::{GLOBAL_BRANCH_ID, LixError};

pub(crate) struct PreparedBranchRefRow {
    pub(crate) row: UntrackedStateRow,
}

pub(crate) fn prepare_branch_ref_row(
    branch_id: &str,
    commit_id: &CommitId,
    timestamp: &str,
) -> Result<PreparedBranchRefRow, LixError> {
    let snapshot = serde_json::json!({
        "id": branch_id,
        "commit_id": commit_id.to_string(),
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
            created_at: crate::common::LixTimestamp::expect_parse("created_at", timestamp),
            updated_at: crate::common::LixTimestamp::expect_parse("updated_at", timestamp),
            global: true,
            branch_id: GLOBAL_BRANCH_ID.to_string(),
        },
    })
}
