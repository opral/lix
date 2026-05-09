use datafusion::error::DataFusionError;

use crate::transaction::types::TransactionWriteRow;
use crate::LixError;

pub(crate) fn reject_read_only_entity_surface(
    schema_key: &str,
    operation: &str,
) -> Result<(), DataFusionError> {
    if schema_key == "lix_directory_descriptor" {
        return Err(read_only_error(
            operation,
            schema_key,
            "Use the writable lix_directory surface to create, update, or delete directories.",
        ));
    }
    if let Some(message) = read_only_schema_message(schema_key) {
        return Err(read_only_error(operation, schema_key, message));
    }
    Ok(())
}

pub(crate) fn reject_read_only_stage_rows(
    rows: &[TransactionWriteRow],
    operation: &str,
) -> Result<(), DataFusionError> {
    for row in rows {
        if let Some(message) = read_only_schema_message(&row.schema_key) {
            return Err(read_only_error(operation, &row.schema_key, message));
        }
    }
    Ok(())
}

fn read_only_error(operation: &str, schema_key: &str, message: &'static str) -> DataFusionError {
    super::error::lix_error_to_datafusion_error(
        LixError::new(
            LixError::CODE_READ_ONLY,
            format!("{operation} cannot write read-only surface '{schema_key}'"),
        )
        .with_hint(message),
    )
}

fn read_only_schema_message(schema_key: &str) -> Option<&'static str> {
    match schema_key {
        "lix_version_descriptor" | "lix_version_ref" => {
            Some("Use the writable lix_version surface to create, update, or delete versions.")
        }
        "lix_file_descriptor" => {
            Some("Use the writable lix_file surface to create, update, or delete files.")
        }
        "lix_binary_blob_ref" => {
            Some("Use the writable lix_file data column to create, update, or delete file contents.")
        }
        "lix_commit"
        | "lix_commit_edge"
        | "lix_change" => Some(
            "Commit graph and changelog surfaces are read-only; Lix creates them when transactions commit.",
        ),
        _ => None,
    }
}
