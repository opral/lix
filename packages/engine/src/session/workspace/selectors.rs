use crate::catalog::{bind_named_relation, RelationBindContext};
use crate::sql::lower_catalog_relation_binding_to_source_sql;
use crate::version_state::DEFAULT_ACTIVE_VERSION_NAME;
use crate::{LixBackend, LixError, Value};

pub(crate) const WORKSPACE_METADATA_TABLE: &str = "lix_internal_workspace_metadata";
const WORKSPACE_ACTIVE_VERSION_ID_KEY: &str = "active_version_id";
const WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY: &str = "active_account_ids";

async fn load_workspace_active_version_id(
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

pub(crate) async fn persist_workspace_selectors(
    backend: &dyn LixBackend,
    active_version_id: Option<&str>,
    active_account_ids: Option<&[String]>,
) -> Result<(), LixError> {
    if let Some(active_version_id) = active_version_id {
        persist_workspace_active_version_id(backend, active_version_id).await?;
    }
    if let Some(active_account_ids) = active_account_ids {
        persist_workspace_active_account_ids(backend, active_account_ids).await?;
    }
    Ok(())
}

async fn load_workspace_metadata_value(
    backend: &dyn LixBackend,
    key: &str,
) -> Result<Option<String>, LixError> {
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
    let binding = bind_named_relation("lix_version", RelationBindContext::default())?
        .expect("lix_version must bind to a catalog relation");
    let source_sql = lower_catalog_relation_binding_to_source_sql(backend.dialect(), &binding)?;
    let query_result = backend
        .execute(
            &format!(
                "SELECT id \
                 FROM ({source_sql}) version_inventory \
                 WHERE name = $1 \
                 ORDER BY id ASC \
                 LIMIT 1"
            ),
            &[Value::Text(DEFAULT_ACTIVE_VERSION_NAME.to_string())],
        )
        .await?;
    let Some(row) = query_result.rows.first() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace active version is missing and no default version named 'main' exists",
        ));
    };
    let Some(Value::Text(version_id)) = row.first() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace default version query returned a non-text id",
        ));
    };
    if !version_id.is_empty() {
        return Ok(version_id.clone());
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "workspace active version is missing and no default version named 'main' exists",
    ))
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
