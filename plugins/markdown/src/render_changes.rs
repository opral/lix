use crate::ROOT_ENTITY_PK;
use crate::common::{
    BlockSnapshotContent, DocumentSnapshotContent, SnapshotContent, snapshot_content_to_json,
};
use crate::exports::lix::plugin::api::{EntityState, PluginError};
use crate::schemas::{BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY};
use crate::{File, single_entity_pk};
use std::collections::{BTreeMap, BTreeSet};

struct RenderRow {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: SnapshotContent,
}

pub(crate) fn render_state(file: File, state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
    render_rows(
        file,
        state.into_iter().map(|row| RenderRow {
            entity_pk: row.entity_pk,
            schema_key: row.schema_key,
            snapshot_content: row.snapshot_content,
        }),
    )
}

fn render_rows(
    file: File,
    rows: impl IntoIterator<Item = RenderRow>,
) -> Result<Vec<u8>, PluginError> {
    let mut document: Option<DocumentSnapshotContent> = None;
    let mut blocks_by_id: BTreeMap<String, BlockSnapshotContent> = BTreeMap::new();
    let mut seen_block_ids = BTreeSet::new();

    for row in rows {
        if row.schema_key != DOCUMENT_SCHEMA_KEY && row.schema_key != BLOCK_SCHEMA_KEY {
            continue;
        }

        if row.schema_key == DOCUMENT_SCHEMA_KEY {
            let entity_pk = single_entity_pk(row.entity_pk)?;
            if entity_pk != ROOT_ENTITY_PK {
                return Err(PluginError::InvalidInput(format!(
                    "unsupported entity_pk '{entity_pk}' for schema_key '{DOCUMENT_SCHEMA_KEY}', expected '{ROOT_ENTITY_PK}'"
                )));
            }
            if document.is_some() {
                return Err(PluginError::InvalidInput(format!(
                    "duplicate entity_pk '{ROOT_ENTITY_PK}' for schema_key '{DOCUMENT_SCHEMA_KEY}'"
                )));
            }

            let snapshot_content =
                snapshot_content_to_json(&row.snapshot_content, "markdown document")?;
            let snapshot: DocumentSnapshotContent = serde_json::from_str(&snapshot_content)
                .map_err(|error| {
                    PluginError::InvalidInput(format!(
                        "invalid snapshot_content for entity_pk '{ROOT_ENTITY_PK}': {error}"
                    ))
                })?;
            if snapshot.id != ROOT_ENTITY_PK {
                return Err(PluginError::InvalidInput(format!(
                    "document snapshot id '{}' does not match expected '{}'",
                    snapshot.id, ROOT_ENTITY_PK
                )));
            }

            document = Some(snapshot);
            continue;
        }

        // BLOCK_SCHEMA_KEY
        let entity_pk = single_entity_pk(row.entity_pk)?;
        if !seen_block_ids.insert(entity_pk.clone()) {
            return Err(PluginError::InvalidInput(format!(
                "duplicate entity_pk '{entity_pk}' for schema_key '{BLOCK_SCHEMA_KEY}'"
            )));
        }

        let snapshot_content = snapshot_content_to_json(&row.snapshot_content, "markdown block")?;
        let snapshot: BlockSnapshotContent =
            serde_json::from_str(&snapshot_content).map_err(|error| {
                PluginError::InvalidInput(format!(
                    "invalid snapshot_content for entity_pk '{entity_pk}': {error}"
                ))
            })?;

        if snapshot.id != entity_pk {
            return Err(PluginError::InvalidInput(format!(
                "block snapshot id '{}' does not match entity_pk '{}'",
                snapshot.id, entity_pk
            )));
        }

        blocks_by_id.insert(entity_pk, snapshot);
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
