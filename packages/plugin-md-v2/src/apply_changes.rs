use crate::common::SnapshotContent;
use crate::exports::lix::plugin::api::{EntityChange, File, PluginError};
use crate::{ROOT_ENTITY_ID, SCHEMA_KEY, SCHEMA_VERSION};

pub(crate) fn apply_changes(
    file: File,
    changes: Vec<EntityChange>,
) -> Result<Vec<u8>, PluginError> {
    let mut root_change: Option<EntityChange> = None;

    for change in changes {
        if change.schema_key != SCHEMA_KEY {
            continue;
        }
        if change.schema_version != SCHEMA_VERSION {
            return Err(PluginError::InvalidInput(format!(
                "unsupported schema_version '{}' for schema_key '{}', expected '{}'",
                change.schema_version, SCHEMA_KEY, SCHEMA_VERSION
            )));
        }
        if change.entity_id != ROOT_ENTITY_ID {
            return Err(PluginError::InvalidInput(format!(
                "unsupported entity_id '{}' for schema_key '{}', expected '{}'",
                change.entity_id, SCHEMA_KEY, ROOT_ENTITY_ID
            )));
        }
        if root_change.is_some() {
            return Err(PluginError::InvalidInput(format!(
                "duplicate entity_id '{}' for schema_key '{}'",
                ROOT_ENTITY_ID, SCHEMA_KEY
            )));
        }
        root_change = Some(change);
    }

    let Some(change) = root_change else {
        return Ok(file.data);
    };

    let Some(snapshot_content) = change.snapshot_content else {
        return Ok(Vec::new());
    };

    let snapshot: SnapshotContent = serde_json::from_str(&snapshot_content).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid snapshot_content for entity_id '{}': {error}",
            ROOT_ENTITY_ID
        ))
    })?;

    Ok(snapshot.markdown.into_bytes())
}
