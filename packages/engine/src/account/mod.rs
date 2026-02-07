use serde_json::Value as JsonValue;
use std::sync::OnceLock;

use crate::builtin_schema::types::{LixAccount, LixActiveAccount};
use crate::builtin_schema::{builtin_schema_definition, builtin_schema_json};
use crate::LixError;

pub(crate) const ACCOUNT_STORAGE_VERSION_ID: &str = "global";

static ACCOUNT_SCHEMA_METADATA: OnceLock<SchemaMetadata> = OnceLock::new();
static ACTIVE_ACCOUNT_SCHEMA_METADATA: OnceLock<SchemaMetadata> = OnceLock::new();

struct SchemaMetadata {
    schema_key: String,
    schema_version: String,
    file_id: String,
    plugin_key: String,
    storage_version_id: String,
}

#[allow(dead_code)]
pub(crate) fn account_schema_definition() -> &'static JsonValue {
    builtin_schema_definition("lix_account").expect("builtin schema 'lix_account' must exist")
}

#[allow(dead_code)]
pub(crate) fn account_schema_definition_json() -> &'static str {
    builtin_schema_json("lix_account").expect("builtin schema 'lix_account' must exist")
}

pub(crate) fn account_schema_key() -> &'static str {
    &account_schema_metadata().schema_key
}

pub(crate) fn account_schema_version() -> &'static str {
    &account_schema_metadata().schema_version
}

pub(crate) fn account_file_id() -> &'static str {
    &account_schema_metadata().file_id
}

pub(crate) fn account_plugin_key() -> &'static str {
    &account_schema_metadata().plugin_key
}

pub(crate) fn account_storage_version_id() -> &'static str {
    &account_schema_metadata().storage_version_id
}

pub(crate) fn account_snapshot_content(id: &str, name: &str) -> String {
    serde_json::to_string(&LixAccount {
        id: id.to_string(),
        name: name.to_string(),
    })
    .expect("lix_account snapshot serialization must succeed")
}

#[allow(dead_code)]
pub(crate) fn active_account_schema_definition() -> &'static JsonValue {
    builtin_schema_definition("lix_active_account")
        .expect("builtin schema 'lix_active_account' must exist")
}

#[allow(dead_code)]
pub(crate) fn active_account_schema_definition_json() -> &'static str {
    builtin_schema_json("lix_active_account")
        .expect("builtin schema 'lix_active_account' must exist")
}

pub(crate) fn active_account_schema_key() -> &'static str {
    &active_account_schema_metadata().schema_key
}

pub(crate) fn active_account_schema_version() -> &'static str {
    &active_account_schema_metadata().schema_version
}

pub(crate) fn active_account_file_id() -> &'static str {
    &active_account_schema_metadata().file_id
}

pub(crate) fn active_account_plugin_key() -> &'static str {
    &active_account_schema_metadata().plugin_key
}

pub(crate) fn active_account_storage_version_id() -> &'static str {
    &active_account_schema_metadata().storage_version_id
}

pub(crate) fn active_account_snapshot_content(account_id: &str) -> String {
    serde_json::to_string(&LixActiveAccount {
        account_id: account_id.to_string(),
    })
    .expect("lix_active_account snapshot serialization must succeed")
}

pub(crate) fn parse_active_account_snapshot(snapshot_content: &str) -> Result<String, LixError> {
    let parsed: LixActiveAccount =
        serde_json::from_str(snapshot_content).map_err(|error| LixError {
            message: format!("active account snapshot_content invalid JSON: {error}"),
        })?;

    if parsed.account_id.is_empty() {
        return Err(LixError {
            message: "active account id must not be empty".to_string(),
        });
    }

    Ok(parsed.account_id)
}

fn account_schema_metadata() -> &'static SchemaMetadata {
    ACCOUNT_SCHEMA_METADATA.get_or_init(|| parse_schema_metadata("lix_account"))
}

fn active_account_schema_metadata() -> &'static SchemaMetadata {
    ACTIVE_ACCOUNT_SCHEMA_METADATA.get_or_init(|| parse_schema_metadata("lix_active_account"))
}

fn parse_schema_metadata(schema_key: &str) -> SchemaMetadata {
    let schema = builtin_schema_definition(schema_key).unwrap_or_else(|| {
        panic!("builtin schema '{schema_key}' must exist");
    });
    let parsed_schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| panic!("builtin schema '{schema_key}' must define string x-lix-key"))
        .to_string();
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| panic!("builtin schema '{schema_key}' must define string x-lix-version"))
        .to_string();
    let overrides = schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
        .unwrap_or_else(|| {
            panic!("builtin schema '{schema_key}' must define object x-lix-override-lixcols")
        });
    let file_id_raw = overrides
        .get("lixcol_file_id")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| {
            panic!("builtin schema '{schema_key}' must define string lixcol_file_id")
        });
    let plugin_key_raw = overrides
        .get("lixcol_plugin_key")
        .and_then(JsonValue::as_str)
        .unwrap_or_else(|| {
            panic!("builtin schema '{schema_key}' must define string lixcol_plugin_key")
        });
    let storage_version_id = overrides
        .get("lixcol_version_id")
        .and_then(JsonValue::as_str)
        .map(decode_lixcol_literal)
        .unwrap_or_else(|| ACCOUNT_STORAGE_VERSION_ID.to_string());

    SchemaMetadata {
        schema_key: parsed_schema_key,
        schema_version,
        file_id: decode_lixcol_literal(file_id_raw),
        plugin_key: decode_lixcol_literal(plugin_key_raw),
        storage_version_id,
    }
}

fn decode_lixcol_literal(raw: &str) -> String {
    serde_json::from_str::<String>(raw).unwrap_or_else(|_| raw.trim_matches('"').to_string())
}
