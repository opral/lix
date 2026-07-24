use datafusion::error::DataFusionError;

use crate::LixError;
use crate::transaction::types::TransactionWriteRow;

pub(crate) fn reject_read_only_entity_surface(
    schema_key: &str,
    action: &str,
) -> Result<(), DataFusionError> {
    if let Some(message) = read_only_entity_surface_hint(schema_key) {
        return Err(read_only_error(action, schema_key, message));
    }
    Ok(())
}

pub(crate) fn is_read_only_entity_surface(schema_key: &str) -> bool {
    read_only_entity_surface_hint(schema_key).is_some()
}

pub(crate) fn read_only_entity_surface_hint(schema_key: &str) -> Option<&'static str> {
    if schema_key == "lix_directory_descriptor" {
        return Some(
            "Use the writable lix_directory surface to create, update, or delete directories.",
        );
    }
    read_only_schema_message(schema_key)
}

pub(crate) fn reject_read_only_stage_rows(
    rows: &[TransactionWriteRow],
    action: &str,
) -> Result<(), DataFusionError> {
    for row in rows {
        if let Some(message) = read_only_schema_message(&row.schema_key) {
            return Err(read_only_error(action, &row.schema_key, message));
        }
    }
    Ok(())
}

fn read_only_error(action: &str, schema_key: &str, message: &'static str) -> DataFusionError {
    super::error::lix_error_to_datafusion_error(
        LixError::new(
            LixError::CODE_READ_ONLY,
            format!("{action} cannot write read-only surface '{schema_key}'"),
        )
        .with_hint(message),
    )
}

fn read_only_schema_message(schema_key: &str) -> Option<&'static str> {
    match schema_key {
        "lix_branch_descriptor" | "lix_branch_ref" => {
            Some("Use the writable lix_branch surface to create, update, or delete branches.")
        }
        "lix_file_descriptor" => {
            Some("Use the writable lix_file surface to create, update, or delete files.")
        }
        "lix_binary_blob_ref" => Some(
            "Use the writable lix_file data column to create, update, or delete file contents.",
        ),
        "lix_commit" | "lix_commit_edge" | "lix_change" => Some(
            "Commit graph and changelog surfaces are read-only; Lix creates them when transactions commit.",
        ),
        "lix_checkpoint_marker" => Some(
            "Checkpoint markers are internal; use the create_checkpoint API to create checkpoints.",
        ),
        _ => None,
    }
}
