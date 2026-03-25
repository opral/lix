use crate::live_state::raw::{load_exact_row_with_executor, RawStorage};
use crate::version::{
    version_descriptor_file_id, version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_storage_version_id,
};
use crate::{Engine, EngineTransaction, ExecuteOptions, LixError, Value};

pub async fn switch_version(engine: &Engine, version_id: String) -> Result<(), LixError> {
    if version_id.trim().is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "version_id must be a non-empty string".to_string(),
        });
    }

    engine
        .transaction(ExecuteOptions::default(), move |tx| {
            let version_id = version_id.clone();
            Box::pin(async move { switch_version_in_transaction(tx, version_id).await })
        })
        .await
}

async fn switch_version_in_transaction(
    tx: &mut EngineTransaction<'_>,
    version_id: String,
) -> Result<(), LixError> {
    ensure_version_exists(tx, &version_id).await?;
    tx.execute(
        "UPDATE lix_active_version SET version_id = $1",
        &[Value::Text(version_id)],
    )
    .await?;
    Ok(())
}

async fn ensure_version_exists(
    tx: &mut EngineTransaction<'_>,
    version_id: &str,
) -> Result<(), LixError> {
    let transaction = tx.transaction.as_mut().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "switch_version expected an open transaction",
        )
    })?;
    let mut executor = transaction.as_mut();
    let row = load_exact_row_with_executor(
        &mut executor,
        RawStorage::Tracked,
        version_descriptor_schema_key(),
        version_descriptor_storage_version_id(),
        version_id,
        Some(version_descriptor_file_id()),
    )
    .await?;
    if row
        .as_ref()
        .is_none_or(|row| row.plugin_key() != version_descriptor_plugin_key())
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("version '{version_id}' does not exist"),
        });
    }
    Ok(())
}
