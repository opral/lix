use crate::backend::QueryExecutor;
use crate::canonical::CanonicalStateIdentity;
use crate::execution::write::filesystem::runtime::{
    ExactFilesystemDescriptorState, FilesystemDescriptorState, FILESYSTEM_DESCRIPTOR_FILE_ID,
    FILESYSTEM_FILE_SCHEMA_KEY,
};
use crate::live_state::key_value_schema_key;
use crate::live_state::{load_exact_untracked_row_with_executor, ExactUntrackedRowRequest};
use crate::runtime::deterministic_mode::deterministic_sequence_key;
use crate::version_state::GLOBAL_VERSION_ID;
use crate::{LixError, Value};

use crate::session::version_ops::load_exact_canonical_row_at_version_head_with_executor;

pub(crate) async fn load_create_commit_deterministic_sequence_start(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<i64>, LixError> {
    let untracked = load_exact_untracked_row_with_executor(
        executor,
        &ExactUntrackedRowRequest {
            schema_key: key_value_schema_key().to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: deterministic_sequence_key().to_string(),
            file_id: Some(FILESYSTEM_DESCRIPTOR_FILE_ID.to_string()),
        },
    )
    .await?;
    if let Some(row) = untracked {
        let Some(raw_value) = row.property_text("value") else {
            return Ok(Some(0));
        };
        let value = raw_value.parse::<i64>().map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "deterministic sequence row contained non-integer value '{raw_value}': {error}"
                ),
            )
        })?;
        return Ok(Some(value));
    }

    let tracked = load_exact_canonical_row_at_version_head_with_executor(
        executor,
        GLOBAL_VERSION_ID,
        &CanonicalStateIdentity {
            entity_id: deterministic_sequence_key().to_string(),
            schema_key: key_value_schema_key().to_string(),
            file_id: FILESYSTEM_DESCRIPTOR_FILE_ID.to_string(),
        },
    )
    .await?;
    let Some(snapshot_content) = tracked.as_ref().map(|row| row.snapshot_content.clone()) else {
        return Ok(Some(0));
    };
    parse_deterministic_sequence_snapshot(&snapshot_content).map(Some)
}

pub(crate) async fn load_untracked_file_descriptor(
    executor: &mut dyn QueryExecutor,
    file_id: &str,
    version_id: &str,
) -> Result<Option<ExactFilesystemDescriptorState>, LixError> {
    let Some(row) = load_exact_untracked_row_with_executor(
        executor,
        &ExactUntrackedRowRequest {
            schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
            version_id: version_id.to_string(),
            entity_id: file_id.to_string(),
            file_id: Some(FILESYSTEM_DESCRIPTOR_FILE_ID.to_string()),
        },
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(ExactFilesystemDescriptorState {
        descriptor: FilesystemDescriptorState {
            directory_id: row.property_text("directory_id").unwrap_or_default(),
            name: row.property_text("name").unwrap_or_default(),
            extension: row
                .property_text("extension")
                .filter(|text| !text.is_empty()),
            hidden: value_as_bool(row.values.get("hidden")).unwrap_or(false),
            metadata: row.metadata.clone(),
        },
        untracked: true,
    }))
}

fn value_as_bool(value: Option<&Value>) -> Option<bool> {
    match value {
        Some(Value::Boolean(value)) => Some(*value),
        Some(Value::Integer(value)) => Some(*value != 0),
        _ => None,
    }
}

fn parse_deterministic_sequence_snapshot(snapshot_content: &str) -> Result<i64, LixError> {
    let parsed: serde_json::Value = serde_json::from_str(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("deterministic sequence snapshot invalid JSON: {error}"),
        )
    })?;
    Ok(parsed
        .get("value")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0))
}
