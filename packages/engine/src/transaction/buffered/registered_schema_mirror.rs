use crate::canonical::CanonicalChangeWrite;
use crate::sql::MutationRow;
use crate::sql::PlannedStateRow;
pub(crate) use crate::transaction::buffered::storage::{
    upsert_registered_schema_mirror_row_in_transaction, RegisteredSchemaMirrorRow,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError, Value};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const PENDING_BOOTSTRAP_TIMESTAMP: &str = "1970-01-01T00:00:00Z";

pub(crate) async fn mirror_registered_schema_planned_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    rows: &[PlannedStateRow],
    canonical_changes: &[CanonicalChangeWrite],
    untracked: bool,
) -> Result<(), LixError> {
    if rows.len() != canonical_changes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "registered schema mirror expected {} canonical changes for {} planned rows",
                rows.len(),
                canonical_changes.len()
            ),
        ));
    }

    for (row, change) in rows.iter().zip(canonical_changes.iter()) {
        if row.schema_key != REGISTERED_SCHEMA_KEY {
            continue;
        }

        let schema_version = planned_row_text_value(row, "schema_version")?;
        let file_id = planned_row_optional_text_value(row, "file_id");
        let plugin_key = planned_row_optional_text_value(row, "plugin_key");
        let metadata = planned_row_optional_text_value(row, "metadata");
        let snapshot_content = if row.tombstone {
            None
        } else {
            planned_row_optional_json_text_value(row, "snapshot_content")?
        };

        upsert_registered_schema_mirror_row_in_transaction(
            transaction,
            RegisteredSchemaMirrorRow {
                entity_id: row.entity_id.as_str(),
                schema_version: schema_version.as_str(),
                file_id,
                version_id: row.version_id.as_deref().unwrap_or(GLOBAL_VERSION_ID),
                plugin_key,
                snapshot_content: snapshot_content.as_deref(),
                metadata,
                change_id: change.id.as_str(),
                untracked,
                created_at: change.created_at.as_str(),
            },
        )
        .await?;
    }

    Ok(())
}

pub(crate) async fn mirror_registered_schema_mutations_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    rows: &[MutationRow],
) -> Result<(), LixError> {
    for row in rows {
        if row.schema_key != REGISTERED_SCHEMA_KEY {
            continue;
        }

        let snapshot_content = row
            .snapshot_content
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to serialize registered schema snapshot: {error}"),
                )
            })?;

        upsert_registered_schema_mirror_row_in_transaction(
            transaction,
            RegisteredSchemaMirrorRow {
                entity_id: row.entity_id.as_str(),
                schema_version: row.schema_version.as_str(),
                file_id: row.file_id.as_deref(),
                version_id: row.version_id.as_str(),
                plugin_key: row.plugin_key.as_deref(),
                snapshot_content: snapshot_content.as_deref(),
                metadata: None,
                change_id: "bootstrap~mutation",
                untracked: row.untracked,
                created_at: PENDING_BOOTSTRAP_TIMESTAMP,
            },
        )
        .await?;
    }

    Ok(())
}
fn planned_row_text_value(row: &PlannedStateRow, key: &str) -> Result<String, LixError> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Ok(value.clone()),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("planned row missing text value for '{key}'"),
        )),
    }
}

fn planned_row_optional_text_value<'a>(row: &'a PlannedStateRow, key: &str) -> Option<&'a str> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn planned_row_optional_json_text_value(
    row: &PlannedStateRow,
    key: &str,
) -> Result<Option<String>, LixError> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(Value::Json(value)) => serde_json::to_string(value).map(Some).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to serialize planned row JSON value for '{key}': {error}"),
            )
        }),
        Some(Value::Null) | None => Ok(None),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("planned row '{key}' value must be text, json, or null"),
        )),
    }
}
