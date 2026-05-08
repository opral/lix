use crate::entity_identity::EntityIdentity;
use crate::json_store::{JsonStoreWriter, NormalizedJson};
use crate::untracked_state::UntrackedStateRow;
use crate::version::VERSION_REF_SCHEMA_KEY;
use crate::{LixError, GLOBAL_VERSION_ID};

pub(crate) fn prepare_version_ref_row(
    json_writer: &mut JsonStoreWriter,
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> Result<UntrackedStateRow, LixError> {
    let snapshot = serde_json::json!({
        "id": version_id,
        "commit_id": commit_id,
    });
    let snapshot_ref = json_writer.prepare_json(NormalizedJson::from_value(
        &snapshot,
        "engine version-ref snapshot_content",
    )?)?;

    Ok(UntrackedStateRow {
        entity_id: EntityIdentity::single(version_id),
        schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_ref: Some(snapshot_ref),
        metadata_ref: None,
        created_at: timestamp.to_string(),
        updated_at: timestamp.to_string(),
        global: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    })
}
