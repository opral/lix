use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonStoreWriter;
use crate::storage::StorageWriteSet;
use crate::untracked_state::UntrackedStateRow;
use crate::version::{VERSION_REF_SCHEMA_KEY, VERSION_REF_SCHEMA_VERSION};
use crate::{LixError, GLOBAL_VERSION_ID};

pub(crate) fn prepare_version_ref_row(
    writes: &mut StorageWriteSet,
    json_writer: &mut JsonStoreWriter,
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> Result<UntrackedStateRow, LixError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": version_id,
        "commit_id": commit_id,
    }))
    .map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("engine2 version-ref snapshot serialization failed: {error}"),
        )
    })?;

    Ok(UntrackedStateRow {
        entity_id: EntityIdentity::single(version_id),
        schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_ref: Some(json_writer.stage_bytes(writes, snapshot_content.as_bytes())?),
        metadata_ref: None,
        schema_version: VERSION_REF_SCHEMA_VERSION.to_string(),
        created_at: timestamp.to_string(),
        updated_at: timestamp.to_string(),
        global: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    })
}
