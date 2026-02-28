use crate::{Engine, EngineTransaction, ExecuteOptions, LixError, Value};

pub async fn switch_version(engine: &Engine, version_id: String) -> Result<(), LixError> {
    if version_id.trim().is_empty() {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "version_id must be a non-empty string".to_string(),
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
    let result = tx
        .execute(
            "SELECT 1 \
             FROM lix_version \
             WHERE id = $1 \
             LIMIT 1",
            &[Value::Text(version_id.to_string())],
        )
        .await?;
    if result.rows.is_empty() {
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!("version '{version_id}' does not exist"),
        });
    }
    Ok(())
}
