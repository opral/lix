use crate::common::{BlockSnapshotContent, DocumentSnapshotContent};
use crate::exports::lix::plugin::api::{EntityChange, File, PluginError};
use crate::schemas::{BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, ENTITY_SCHEMA_VERSION};
use crate::ROOT_ENTITY_ID;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn apply_changes(
    file: File,
    changes: Vec<EntityChange>,
) -> Result<Vec<u8>, PluginError> {
    let mut document: Option<DocumentSnapshotContent> = None;
    let mut blocks_by_id: BTreeMap<String, BlockSnapshotContent> = BTreeMap::new();
    let mut seen_block_ids = BTreeSet::new();

    for change in changes {
        if change.schema_key != DOCUMENT_SCHEMA_KEY && change.schema_key != BLOCK_SCHEMA_KEY {
            continue;
        }

        if change.schema_version != ENTITY_SCHEMA_VERSION {
            return Err(PluginError::InvalidInput(format!(
                "unsupported schema_version '{}' for schema_key '{}', expected '{}'",
                change.schema_version, change.schema_key, ENTITY_SCHEMA_VERSION
            )));
        }

        if change.schema_key == DOCUMENT_SCHEMA_KEY {
            if change.entity_id != ROOT_ENTITY_ID {
                return Err(PluginError::InvalidInput(format!(
                    "unsupported entity_id '{}' for schema_key '{}', expected '{}'",
                    change.entity_id, DOCUMENT_SCHEMA_KEY, ROOT_ENTITY_ID
                )));
            }
            if document.is_some() {
                return Err(PluginError::InvalidInput(format!(
                    "duplicate entity_id '{}' for schema_key '{}'",
                    ROOT_ENTITY_ID, DOCUMENT_SCHEMA_KEY
                )));
            }

            let snapshot = match change.snapshot_content {
                Some(raw) => {
                    let parsed: DocumentSnapshotContent =
                        serde_json::from_str(&raw).map_err(|error| {
                            PluginError::InvalidInput(format!(
                                "invalid snapshot_content for entity_id '{}': {error}",
                                ROOT_ENTITY_ID
                            ))
                        })?;
                    if parsed.id != ROOT_ENTITY_ID {
                        return Err(PluginError::InvalidInput(format!(
                            "document snapshot id '{}' does not match expected '{}'",
                            parsed.id, ROOT_ENTITY_ID
                        )));
                    }
                    parsed
                }
                None => DocumentSnapshotContent {
                    id: ROOT_ENTITY_ID.to_string(),
                    order: Vec::new(),
                },
            };

            document = Some(snapshot);
            continue;
        }

        // BLOCK_SCHEMA_KEY
        if !seen_block_ids.insert(change.entity_id.clone()) {
            return Err(PluginError::InvalidInput(format!(
                "duplicate entity_id '{}' for schema_key '{}'",
                change.entity_id, BLOCK_SCHEMA_KEY
            )));
        }

        let Some(snapshot_content) = change.snapshot_content else {
            continue;
        };

        let snapshot: BlockSnapshotContent =
            serde_json::from_str(&snapshot_content).map_err(|error| {
                PluginError::InvalidInput(format!(
                    "invalid snapshot_content for entity_id '{}': {error}",
                    change.entity_id
                ))
            })?;

        if snapshot.id != change.entity_id {
            return Err(PluginError::InvalidInput(format!(
                "block snapshot id '{}' does not match entity_id '{}'",
                snapshot.id, change.entity_id
            )));
        }

        blocks_by_id.insert(change.entity_id, snapshot);
    }

    if document.is_none() && blocks_by_id.is_empty() {
        return Ok(file.data);
    }

    let mut ordered_ids = document
        .as_ref()
        .map(|doc| doc.order.clone())
        .unwrap_or_else(|| blocks_by_id.keys().cloned().collect());

    // Include orphaned blocks not referenced by document order to avoid data loss.
    for id in blocks_by_id.keys() {
        if !ordered_ids.contains(id) {
            ordered_ids.push(id.clone());
        }
    }

    let mut parts = Vec::new();
    for id in ordered_ids {
        let Some(block) = blocks_by_id.get(&id) else {
            continue;
        };
        let normalized = block.markdown.trim_matches('\n').to_string();
        if normalized.is_empty() {
            continue;
        }
        parts.push(normalized);
    }

    let mut markdown = parts.join("\n\n");
    if !markdown.is_empty() {
        markdown.push('\n');
    }

    Ok(markdown.into_bytes())
}
