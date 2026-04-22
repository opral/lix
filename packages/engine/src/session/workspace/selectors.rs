use crate::{LixBackend, LixError};

pub(crate) async fn require_workspace_active_version_id(
    backend: &dyn LixBackend,
) -> Result<String, LixError> {
    if let Some(version_id) =
        crate::session::workspace::storage::load_workspace_active_version_id(backend).await?
    {
        return Ok(version_id);
    }

    let version_id =
        crate::session::workspace::storage::load_default_workspace_active_version_id(backend)
            .await?;
    crate::session::workspace::storage::persist_workspace_active_version_id(backend, &version_id)
        .await?;
    Ok(version_id)
}

pub(crate) async fn load_workspace_active_account_ids(
    backend: &dyn LixBackend,
) -> Result<Option<Vec<String>>, LixError> {
    let Some(raw_value) =
        crate::session::workspace::storage::load_workspace_active_account_ids_json(backend).await?
    else {
        return Ok(None);
    };
    parse_workspace_active_account_ids_json(&raw_value).map(Some)
}

pub(crate) async fn persist_workspace_active_version_id(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<(), LixError> {
    crate::session::workspace::storage::persist_workspace_active_version_id(backend, version_id)
        .await
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
    crate::session::workspace::storage::persist_workspace_active_account_ids_json(backend, &encoded)
        .await
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
