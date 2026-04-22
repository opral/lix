use crate::backend::execute_ddl_batch;
use crate::version::load_all_version_descriptors_with_executor;
use crate::{LixBackend, LixError, Value};

pub(crate) const WORKSPACE_METADATA_TABLE: &str = "lix_internal_workspace_metadata";
pub(crate) const DEFAULT_ACTIVE_VERSION_NAME: &str = "main";

const WORKSPACE_ACTIVE_VERSION_ID_KEY: &str = "active_version_id";
const WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY: &str = "active_account_ids";

pub(crate) async fn init_workspace_metadata_storage(
    backend: &dyn LixBackend,
) -> Result<(), LixError> {
    let statements = [format!(
        "CREATE TABLE {} (\
         key TEXT PRIMARY KEY, \
         value TEXT NOT NULL\
         )",
        WORKSPACE_METADATA_TABLE
    )];
    let statement_refs = statements.iter().map(String::as_str).collect::<Vec<_>>();
    execute_ddl_batch(backend, "workspace", &statement_refs).await
}

pub(crate) async fn load_workspace_active_version_id(
    backend: &dyn LixBackend,
) -> Result<Option<String>, LixError> {
    load_workspace_metadata_value(backend, WORKSPACE_ACTIVE_VERSION_ID_KEY).await
}

pub(crate) async fn load_workspace_active_account_ids_json(
    backend: &dyn LixBackend,
) -> Result<Option<String>, LixError> {
    load_workspace_metadata_value(backend, WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY).await
}

pub(crate) async fn persist_workspace_active_version_id(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<(), LixError> {
    persist_workspace_metadata_value(backend, WORKSPACE_ACTIVE_VERSION_ID_KEY, version_id).await
}

pub(crate) async fn persist_workspace_active_account_ids_json(
    backend: &dyn LixBackend,
    account_ids_json: &str,
) -> Result<(), LixError> {
    persist_workspace_metadata_value(backend, WORKSPACE_ACTIVE_ACCOUNT_IDS_KEY, account_ids_json)
        .await
}

pub(crate) async fn load_default_workspace_active_version_id(
    backend: &dyn LixBackend,
) -> Result<String, LixError> {
    let mut executor = backend;
    let mut matching_descriptors = load_all_version_descriptors_with_executor(&mut executor)
        .await?
        .into_iter()
        .filter(|descriptor| descriptor.name == DEFAULT_ACTIVE_VERSION_NAME)
        .collect::<Vec<_>>();
    matching_descriptors.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    let Some(descriptor) = matching_descriptors.first() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "workspace active version is missing and no default version named 'main' exists",
        ));
    };
    Ok(descriptor.version_id.clone())
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
