use crate::entity_identity::EntityIdentity;
use crate::json_store::{JsonRef, NormalizedJson};
use crate::untracked_state::UntrackedStateRow;
use crate::version::VERSION_REF_SCHEMA_KEY;
use crate::{LixError, GLOBAL_VERSION_ID};

pub(crate) struct PreparedVersionRefRow {
    pub(crate) row: UntrackedStateRow,
    pub(crate) snapshot: NormalizedJson,
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
    let snapshot = NormalizedJson::from_value(&snapshot, "engine version-ref snapshot_content")?;
    let snapshot_ref = JsonRef::for_content(snapshot.as_bytes());

    Ok(PreparedVersionRefRow {
        row: UntrackedStateRow {
            entity_id: EntityIdentity::single(version_id),
            schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_ref: Some(snapshot_ref),
            metadata_ref: None,
            created_at: timestamp.to_string(),
            updated_at: timestamp.to_string(),
            global: true,
            version_id: GLOBAL_VERSION_ID.to_string(),
        },
        snapshot,
    })
}
