use crate::common::text::escape_sql_string;
use crate::contracts::artifacts::{MutationRow, PlannedStateRow};
use crate::{LixBackendTransaction, LixError, Value};

const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_registered_schema_bootstrap";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const GLOBAL_VERSION_ID: &str = "global";
const PENDING_BOOTSTRAP_TIMESTAMP: &str = "1970-01-01T00:00:00Z";

pub(super) async fn mirror_registered_schema_planned_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    rows: &[PlannedStateRow],
    untracked: bool,
) -> Result<(), LixError> {
    for row in rows {
        if row.schema_key != REGISTERED_SCHEMA_KEY {
            continue;
        }

        let schema_version = planned_row_text_value(row, "schema_version")?;
        let file_id = planned_row_text_value(row, "file_id")?;
        let plugin_key = planned_row_text_value(row, "plugin_key")?;
        let metadata = planned_row_optional_text_value(row, "metadata");
        let snapshot_content = if row.tombstone {
            None
        } else {
            planned_row_optional_json_text_value(row, "snapshot_content")?
        };

        upsert_registered_schema_bootstrap_row_in_transaction(
            transaction,
            RegisteredSchemaBootstrapRow {
                entity_id: row.entity_id.as_str(),
                schema_version: schema_version.as_str(),
                file_id: file_id.as_str(),
                version_id: row.version_id.as_deref().unwrap_or(GLOBAL_VERSION_ID),
                plugin_key: plugin_key.as_str(),
                snapshot_content: snapshot_content.as_deref(),
                metadata,
                untracked,
            },
        )
        .await?;
    }

    Ok(())
}

pub(super) async fn mirror_registered_schema_mutations_in_transaction(
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

        upsert_registered_schema_bootstrap_row_in_transaction(
            transaction,
            RegisteredSchemaBootstrapRow {
                entity_id: row.entity_id.as_str(),
                schema_version: row.schema_version.as_str(),
                file_id: row.file_id.as_str(),
                version_id: row.version_id.as_str(),
                plugin_key: row.plugin_key.as_str(),
                snapshot_content: snapshot_content.as_deref(),
                metadata: None,
                untracked: row.untracked,
            },
        )
        .await?;
    }

    Ok(())
}

struct RegisteredSchemaBootstrapRow<'a> {
    entity_id: &'a str,
    schema_version: &'a str,
    file_id: &'a str,
    version_id: &'a str,
    plugin_key: &'a str,
    snapshot_content: Option<&'a str>,
    metadata: Option<&'a str>,
    untracked: bool,
}

async fn upsert_registered_schema_bootstrap_row_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    row: RegisteredSchemaBootstrapRow<'_>,
) -> Result<(), LixError> {
    let snapshot_sql = row
        .snapshot_content
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let metadata_sql = row
        .metadata
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let synthetic_change_id = format!("bootstrap~{}", row.entity_id);
    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, is_tombstone, untracked, created_at, updated_at\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', {global}, '{plugin_key}', {snapshot_content}, '{change_id}', {metadata}, {is_tombstone}, {untracked}, '{created_at}', '{updated_at}'\
         ) ON CONFLICT (entity_id, file_id, version_id, untracked) DO UPDATE SET \
         schema_key = excluded.schema_key, \
         schema_version = excluded.schema_version, \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         snapshot_content = excluded.snapshot_content, \
         change_id = excluded.change_id, \
         metadata = excluded.metadata, \
         is_tombstone = excluded.is_tombstone, \
         updated_at = excluded.updated_at",
        table = REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
        entity_id = escape_sql_string(row.entity_id),
        schema_key = REGISTERED_SCHEMA_KEY,
        schema_version = escape_sql_string(row.schema_version),
        file_id = escape_sql_string(row.file_id),
        version_id = escape_sql_string(row.version_id),
        global = if row.version_id == GLOBAL_VERSION_ID {
            "true"
        } else {
            "false"
        },
        plugin_key = escape_sql_string(row.plugin_key),
        snapshot_content = snapshot_sql,
        change_id = escape_sql_string(&synthetic_change_id),
        metadata = metadata_sql,
        is_tombstone = if row.snapshot_content.is_some() { 0 } else { 1 },
        untracked = if row.untracked { "true" } else { "false" },
        created_at = PENDING_BOOTSTRAP_TIMESTAMP,
        updated_at = PENDING_BOOTSTRAP_TIMESTAMP,
    );
    transaction.execute(&sql, &[]).await?;
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
