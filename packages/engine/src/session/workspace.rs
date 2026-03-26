use crate::account::{
    active_account_file_id, active_account_schema_key, active_account_storage_version_id,
};
use crate::key_value::{key_value_file_id, key_value_schema_key};
use crate::live_state::{
    builtin_live_table_layout, live_column_name_for_property, untracked_live_table_name,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
};
use crate::{LixBackend, LixError, Value};

const WORKSPACE_STATE_TABLE: &str = "lix_internal_workspace_state";
const WORKSPACE_ACTIVE_VERSION_ID_KEY: &str = "lix_workspace_active_version_id";
const WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY: &str = "lix_workspace_active_account_ids";

pub(crate) async fn load_workspace_active_version_id(
    backend: &dyn LixBackend,
) -> Result<Option<String>, LixError> {
    ensure_workspace_state_table(backend).await?;
    let result = backend
        .execute(
            &format!(
                "SELECT value FROM {WORKSPACE_STATE_TABLE} WHERE key = $1 LIMIT 1"
            ),
            &[Value::Text(WORKSPACE_ACTIVE_VERSION_ID_KEY.to_string())],
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
            format!("workspace active version value must be text, got {other:?}"),
        )),
    }
}

pub(crate) async fn require_workspace_active_version_id(
    backend: &dyn LixBackend,
) -> Result<String, LixError> {
    if let Some(version_id) = load_workspace_active_version_id(backend).await? {
        return Ok(version_id);
    }

    if let Some(version_id) = load_legacy_workspace_active_version_id(backend).await? {
        persist_workspace_active_version_id(backend, &version_id).await?;
        return Ok(version_id);
    }

    let version_id = load_compat_active_version_id(backend).await?.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "workspace active version is missing".to_string(),
    })?;
    persist_workspace_active_version_id(backend, &version_id).await?;
    Ok(version_id)
}

pub(crate) async fn load_workspace_active_account_ids(
    backend: &dyn LixBackend,
) -> Result<Option<Vec<String>>, LixError> {
    ensure_workspace_state_table(backend).await?;
    let result = backend
        .execute(
            &format!(
                "SELECT value FROM {WORKSPACE_STATE_TABLE} WHERE key = $1 LIMIT 1"
            ),
            &[Value::Text(WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY.to_string())],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(Value::Text(value)) = row.first() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace active account ids value must be text",
        ));
    };
    parse_workspace_active_account_ids_json(value).map(Some)
}

pub(crate) async fn load_effective_workspace_active_account_ids(
    backend: &dyn LixBackend,
) -> Result<Vec<String>, LixError> {
    if let Some(account_ids) = load_workspace_active_account_ids(backend).await? {
        return Ok(account_ids);
    }

    let account_ids = load_compat_active_account_ids(backend).await?;
    persist_workspace_active_account_ids(backend, &account_ids).await?;
    Ok(account_ids)
}

pub(crate) async fn persist_workspace_active_version_id(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<(), LixError> {
    ensure_workspace_state_table(backend).await?;
    backend
        .execute(
            &format!(
                "INSERT INTO {WORKSPACE_STATE_TABLE} (key, value) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value"
            ),
            &[
                Value::Text(WORKSPACE_ACTIVE_VERSION_ID_KEY.to_string()),
                Value::Text(version_id.to_string()),
            ],
        )
        .await?;
    Ok(())
}

pub(crate) async fn persist_workspace_active_account_ids(
    backend: &dyn LixBackend,
    account_ids: &[String],
) -> Result<(), LixError> {
    ensure_workspace_state_table(backend).await?;
    let encoded = serde_json::to_string(account_ids).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace active account ids serialization failed: {error}"),
        )
    })?;
    backend
        .execute(
            &format!(
                "INSERT INTO {WORKSPACE_STATE_TABLE} (key, value) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value"
            ),
            &[
                Value::Text(WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY.to_string()),
                Value::Text(encoded),
            ],
        )
        .await?;
    Ok(())
}

async fn ensure_workspace_state_table(backend: &dyn LixBackend) -> Result<(), LixError> {
    backend
        .execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {WORKSPACE_STATE_TABLE} (\
                 key TEXT PRIMARY KEY, \
                 value TEXT NOT NULL\
                 )"
            ),
            &[],
        )
        .await?;
    Ok(())
}

async fn load_legacy_workspace_active_version_id(
    backend: &dyn LixBackend,
) -> Result<Option<String>, LixError> {
    let layout = builtin_live_table_layout(key_value_schema_key())?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "builtin key-value schema must compile to a live layout",
        )
    })?;
    let value_column = live_column_name_for_property(&layout, "value").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "key-value live layout is missing value",
        )
    })?;
    let result = backend
        .execute(
            &format!(
                "SELECT {value_column} \
                 FROM {table_name} \
                 WHERE entity_id = $1 \
                   AND file_id = $2 \
                   AND version_id = 'global' \
                   AND untracked = true \
                   AND {value_column} IS NOT NULL \
                 ORDER BY updated_at DESC \
                 LIMIT 1",
                value_column = value_column,
                table_name = untracked_live_table_name(key_value_schema_key()),
            ),
            &[
                Value::Text(WORKSPACE_ACTIVE_VERSION_ID_KEY.to_string()),
                Value::Text(key_value_file_id().to_string()),
            ],
        )
        .await?;

    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(raw_value) = row.first() else {
        return Ok(None);
    };
    let raw_json = match raw_value {
        Value::Text(value) => value,
        other => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("workspace active version legacy value must be text JSON, got {other:?}"),
            ))
        }
    };
    let parsed: serde_json::Value = serde_json::from_str(raw_json).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace active version legacy value is invalid JSON: {error}"),
        )
    })?;
    Ok(parsed.as_str().map(ToString::to_string))
}

async fn load_compat_active_version_id(backend: &dyn LixBackend) -> Result<Option<String>, LixError> {
    let layout = builtin_live_table_layout(active_version_schema_key())?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "builtin active version schema must compile to a live layout",
        )
    })?;
    let payload_version_column = live_column_name_for_property(&layout, "version_id")
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "active version live layout is missing version_id",
            )
        })?;
    let result = backend
        .execute(
            &format!(
                "SELECT {payload_version_column} \
                 FROM {table_name} \
                 WHERE file_id = $1 \
                   AND version_id = $2 \
                   AND untracked = true \
                   AND {payload_version_column} IS NOT NULL \
                 ORDER BY updated_at DESC \
                 LIMIT 1",
                payload_version_column = payload_version_column,
                table_name = untracked_live_table_name(active_version_schema_key()),
            ),
            &[
                Value::Text(active_version_file_id().to_string()),
                Value::Text(active_version_storage_version_id().to_string()),
            ],
        )
        .await?;

    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(active_version_id) = row.first() else {
        return Ok(None);
    };
    match active_version_id {
        Value::Text(value) if !value.is_empty() => Ok(Some(value.clone())),
        Value::Text(_) => Ok(None),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("active version id must be text, got {other:?}"),
        }),
    }
}

pub(crate) async fn load_compat_active_account_ids(
    backend: &dyn LixBackend,
) -> Result<Vec<String>, LixError> {
    let layout = builtin_live_table_layout(active_account_schema_key())?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "builtin active account schema must compile to a live layout",
        )
    })?;
    let account_id_column = live_column_name_for_property(&layout, "account_id").ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "active account live layout is missing account_id",
        )
    })?;
    let result = backend
        .execute(
            &format!(
                "SELECT {account_id_column} \
                 FROM {table_name} \
                 WHERE file_id = $1 \
                   AND version_id = $2 \
                   AND untracked = true \
                   AND {account_id_column} IS NOT NULL \
                 ORDER BY updated_at DESC",
                account_id_column = account_id_column,
                table_name = untracked_live_table_name(active_account_schema_key()),
            ),
            &[
                Value::Text(active_account_file_id().to_string()),
                Value::Text(active_account_storage_version_id().to_string()),
            ],
        )
        .await?;

    let mut deduped = std::collections::BTreeSet::new();
    for row in result.rows {
        let Some(value) = row.first() else {
            continue;
        };
        match value {
            Value::Text(text) if !text.is_empty() => {
                deduped.insert(text.clone());
            }
            Value::Text(_) | Value::Null => {}
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("active account id must be text, got {other:?}"),
                ))
            }
        }
    }
    Ok(deduped.into_iter().collect())
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
