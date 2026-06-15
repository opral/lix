//! Plugin archive installation.
//!
//! Installing a plugin is a normal tracked write: the declared schemas become
//! `lix_registered_schema` rows and the original archive is stored under the
//! reserved plugin filesystem root.

use serde_json::{Value as JsonValue, json};

use crate::LixError;
use crate::plugin::{
    ParsedPluginArchive, parse_plugin_archive_for_install, plugin_key_from_archive_path,
};
use crate::schema::{
    registered_schema_entity_pk, schema_key_from_definition, validate_lix_schema_definition,
};
use crate::transaction::types::{TransactionJson, TransactionWriteRow};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

pub(crate) fn plugin_schema_rows_from_archive_path(
    archive_path: &str,
    archive_bytes: &[u8],
    branch_id: &str,
    global: bool,
    untracked: bool,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    let plugin_key = plugin_key_from_archive_path(archive_path).ok_or_else(|| {
        LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!("plugin archive path '{archive_path}' is not a valid plugin storage path"),
        )
    })?;
    let parsed = parse_plugin_archive_for_install(archive_bytes)?;
    if parsed.manifest.key != plugin_key {
        return Err(LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!(
                "plugin archive path key '{}' does not match manifest key '{}'",
                plugin_key, parsed.manifest.key
            ),
        ));
    }
    plugin_schema_rows(&parsed, branch_id, global, untracked)
}

fn plugin_schema_rows(
    parsed: &ParsedPluginArchive,
    branch_id: &str,
    global: bool,
    untracked: bool,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    parsed
        .schemas
        .iter()
        .map(|schema| registered_schema_row(schema, branch_id, global, untracked))
        .collect()
}

fn registered_schema_row(
    schema: &JsonValue,
    branch_id: &str,
    global: bool,
    untracked: bool,
) -> Result<TransactionWriteRow, LixError> {
    validate_lix_schema_definition(schema)?;
    let schema_key = schema_key_from_definition(schema)?;
    let entity_pk = registered_schema_entity_pk(&schema_key.schema_key)?;
    Ok(TransactionWriteRow {
        entity_pk: Some(entity_pk),
        schema_key: REGISTERED_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value(
            json!({ "value": schema }),
            "plugin install registered schema snapshot",
        )?),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global,
        change_id: None,
        commit_id: None,
        untracked,
        branch_id: branch_id.to_string(),
    })
}
