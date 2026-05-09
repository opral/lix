use crate::entity_identity::EntityIdentity;
use crate::untracked_state::UntrackedStateRow;
use crate::version::VERSION_REF_SCHEMA_KEY;
use crate::{LixError, GLOBAL_VERSION_ID};

pub(crate) struct PreparedVersionRefRow {
    pub(crate) row: UntrackedStateRow,
}

pub(crate) fn prepare_version_ref_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> Result<PreparedVersionRefRow, LixError> {
    let snapshot = serde_json::json!({
        "id": version_id,
        "commit_id": commit_id,
    });
    let snapshot = crate::json_store::NormalizedJson::from_value(
        &snapshot,
        "engine version-ref snapshot_content",
    )?;

    Ok(PreparedVersionRefRow {
        row: UntrackedStateRow {
            entity_id: EntityIdentity::single(version_id),
            schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot.as_str().to_string()),
            metadata: None,
            created_at: timestamp.to_string(),
            updated_at: timestamp.to_string(),
            global: true,
            version_id: GLOBAL_VERSION_ID.to_string(),
        },
    })
}
