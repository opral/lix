use crate::live_state::{
    builtin_live_table_layout, live_column_name_for_property, tracked_live_table_name,
};
use crate::version::{
    version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id, DEFAULT_ACTIVE_VERSION_NAME,
};
use crate::{LixBackend, LixError, Value};

const WORKSPACE_METADATA_TABLE: &str = "lix_internal_workspace_metadata";
const WORKSPACE_ACTIVE_VERSION_ID_KEY: &str = "active_version_id";
const WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY: &str = "active_account_ids";

pub(crate) async fn load_workspace_active_version_id(
    backend: &dyn LixBackend,
) -> Result<Option<String>, LixError> {
    load_workspace_metadata_value(backend, WORKSPACE_ACTIVE_VERSION_ID_KEY).await
}

pub(crate) async fn require_workspace_active_version_id(
    backend: &dyn LixBackend,
) -> Result<String, LixError> {
    if let Some(version_id) = load_workspace_active_version_id(backend).await? {
        return Ok(version_id);
    }

    let version_id = load_default_workspace_active_version_id(backend).await?;
    persist_workspace_active_version_id(backend, &version_id).await?;
    Ok(version_id)
}

pub(crate) async fn load_workspace_active_account_ids(
    backend: &dyn LixBackend,
) -> Result<Option<Vec<String>>, LixError> {
    let Some(raw_value) =
        load_workspace_metadata_value(backend, WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY).await?
    else {
        return Ok(None);
    };
    parse_workspace_active_account_ids_json(&raw_value).map(Some)
}

pub(crate) async fn persist_workspace_active_version_id(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<(), LixError> {
    persist_workspace_metadata_value(backend, WORKSPACE_ACTIVE_VERSION_ID_KEY, version_id).await
}

pub(crate) async fn persist_workspace_active_account_ids(
    backend: &dyn LixBackend,
    account_ids: &[String],
) -> Result<(), LixError> {
    let encoded = serde_json::to_string(account_ids).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace active account ids serialization failed: {error}"),
        )
    })?;
    persist_workspace_metadata_value(backend, WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY, &encoded).await
}

async fn ensure_workspace_metadata_table(backend: &dyn LixBackend) -> Result<(), LixError> {
    backend
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {WORKSPACE_METADATA_TABLE} (\
                 key TEXT PRIMARY KEY, \
                 value TEXT NOT NULL\
                 )"
            ),
            &[],
        )
        .await?;
    Ok(())
}

async fn load_workspace_metadata_value(
    backend: &dyn LixBackend,
    key: &str,
) -> Result<Option<String>, LixError> {
    ensure_workspace_metadata_table(backend).await?;
    let result = backend
        .execute(
            &format!("SELECT value FROM {WORKSPACE_METADATA_TABLE} WHERE key = $1 LIMIT 1"),
            &[Value::Text(key.to_string())],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    match row.first() {
        Some(Value::Text(value)) if !value.is_empty() => Ok(Some(value.clone())),
        Some(Value::Text(_)) | None => Ok(None),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace metadata value must be text, got {other:?}"),
        )),
    }
}

async fn persist_workspace_metadata_value(
    backend: &dyn LixBackend,
    key: &str,
    value: &str,
) -> Result<(), LixError> {
    ensure_workspace_metadata_table(backend).await?;
    backend
        .execute(
            &format!(
                "INSERT INTO {WORKSPACE_METADATA_TABLE} (key, value) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value"
            ),
            &[Value::Text(key.to_string()), Value::Text(value.to_string())],
        )
        .await?;
    Ok(())
}

async fn load_default_workspace_active_version_id(
    backend: &dyn LixBackend,
) -> Result<String, LixError> {
    let layout = builtin_live_table_layout(version_descriptor_schema_key())?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "builtin version descriptor schema must compile to a live layout",
        )
    })?;
    let name_column = live_column_name_for_property(&layout, "name").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "version descriptor live layout is missing name",
        )
    })?;
    let result = backend
        .execute(
            &format!(
                "SELECT entity_id \
                 FROM {table_name} \
                 WHERE schema_key = $1 \
                   AND file_id = $2 \
                   AND version_id = $3 \
                   AND is_tombstone = 0 \
                   AND {name_column} = $4 \
                 LIMIT 1",
                table_name = tracked_live_table_name(version_descriptor_schema_key()),
                name_column = name_column,
            ),
            &[
                Value::Text(version_descriptor_schema_key().to_string()),
                Value::Text(version_descriptor_file_id().to_string()),
                Value::Text(version_descriptor_storage_version_id().to_string()),
                Value::Text(DEFAULT_ACTIVE_VERSION_NAME.to_string()),
            ],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace active version is missing and no default version named 'main' exists",
        ));
    };
    match row.first() {
        Some(Value::Text(version_id)) if !version_id.is_empty() => Ok(version_id.clone()),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("default workspace active version id must be text, got {other:?}"),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "default workspace active version query returned no entity_id",
        )),
    }
}

fn parse_workspace_active_account_ids_json(raw: &str) -> Result<Vec<String>, LixError> {
    let parsed: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace active account ids value is invalid JSON: {error}"),
        )
    })?;
    let array = parsed.as_array().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace active account ids value must be a JSON array",
        )
    })?;
    let mut deduped = std::collections::BTreeSet::new();
    for value in array {
        let Some(account_id) = value.as_str() else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "workspace active account ids must contain only strings",
            ));
        };
        if !account_id.is_empty() {
            deduped.insert(account_id.to_string());
        }
    }
    Ok(deduped.into_iter().collect())
}
