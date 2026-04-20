use crate::functions::FunctionBindings;
use crate::version::GLOBAL_VERSION_ID;
use crate::{ExecuteOptions, LixError, Session, SessionTransaction, Value};

use super::context::require_target_version_context_in_transaction;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct CreateVersionOptions {
    pub id: Option<String>,
    pub name: Option<String>,
    pub source_version_id: Option<String>,
    #[serde(default)]
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateVersionResult {
    pub id: String,
    pub name: String,
    pub parent_version_id: String,
    pub parent_commit_id: String,
}

pub(crate) async fn create_version_in_session(
    session: &Session,
    options: CreateVersionOptions,
) -> Result<CreateVersionResult, LixError> {
    session
        .transaction(ExecuteOptions::default(), |tx| {
            Box::pin(async move { create_version_in_transaction(tx, options).await })
        })
        .await
}

async fn create_version_in_transaction(
    tx: &mut SessionTransaction<'_>,
    options: CreateVersionOptions,
) -> Result<CreateVersionResult, LixError> {
    let source_context = require_target_version_context_in_transaction(
        tx,
        options.source_version_id.as_deref(),
        "source_version_id",
        "source version",
    )
    .await?;
    let parent_version_id = source_context.version_id().to_string();
    let parent_commit_id = source_context.head_commit_id().to_string();

    let id =
        normalize_optional_non_empty_text(options.id, "id")?.unwrap_or(generate_uuid(tx).await?);
    let name = normalize_optional_non_empty_text(options.name, "name")?.unwrap_or(id.clone());
    if id == GLOBAL_VERSION_ID {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "version id 'global' is reserved".to_string(),
            hint: None,
        });
    }
    let hidden = options.hidden;

    tx.execute(
        "INSERT INTO lix_version (\
         id, name, hidden, commit_id\
         ) VALUES ($1, $2, $3, $4)",
        &[
            Value::Text(id.clone()),
            Value::Text(name.clone()),
            Value::Boolean(hidden),
            Value::Text(parent_commit_id.clone()),
        ],
    )
    .await?;

    Ok(CreateVersionResult {
        id,
        name,
        parent_version_id,
        parent_commit_id,
    })
}

async fn generate_uuid(tx: &mut SessionTransaction<'_>) -> Result<String, LixError> {
    Ok(version_create_function_bindings(tx)
        .await?
        .provider()
        .call_uuid_v7())
}

async fn version_create_function_bindings(
    tx: &mut SessionTransaction<'_>,
) -> Result<FunctionBindings, LixError> {
    if let Some(function_bindings) = tx.context.function_bindings().cloned() {
        return Ok(function_bindings);
    }

    let session_host = tx.session_host();
    let backend = crate::backend::transaction_backend_view(tx.backend_transaction_mut()?);
    let function_bindings =
        crate::session::host::prepare_function_bindings_with_host(session_host, &backend).await?;
    let mut runtime_functions = function_bindings.provider().clone();
    crate::transaction::ensure_runtime_sequence_initialized_in_transaction(
        tx.backend_transaction_mut()?,
        &mut runtime_functions,
    )
    .await?;
    tx.context.set_function_bindings(function_bindings.clone());
    Ok(function_bindings)
}

fn normalize_optional_non_empty_text(
    value: Option<String>,
    field: &str,
) -> Result<Option<String>, LixError> {
    match value {
        Some(value) if value.trim().is_empty() => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{field} must be a non-empty string when provided"),
            hint: None,
        }),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::CreateVersionOptions;

    #[test]
    fn create_version_options_deserialize_defaults_hidden() {
        let options: CreateVersionOptions =
            serde_json::from_str(r#"{"id":"test-version"}"#).expect("deserialization should work");
        assert_eq!(options.id.as_deref(), Some("test-version"));
        assert_eq!(options.source_version_id, None);
        assert!(!options.hidden);
    }

    #[test]
    fn create_version_options_deserialize_source_version_id() {
        let options: CreateVersionOptions =
            serde_json::from_str(r#"{"source_version_id":"base-version"}"#)
                .expect("deserialization should work");
        assert_eq!(options.source_version_id.as_deref(), Some("base-version"));
        assert!(!options.hidden);
    }
}
