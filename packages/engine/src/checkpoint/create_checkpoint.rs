use crate::{errors, Engine, EngineTransaction, ExecuteOptions, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateCheckpointResult {
    pub id: String,
    pub change_set_id: String,
}

pub async fn create_checkpoint(engine: &Engine) -> Result<CreateCheckpointResult, LixError> {
    engine
        .transaction(ExecuteOptions::default(), |tx| {
            Box::pin(async move { create_checkpoint_in_transaction(tx).await })
        })
        .await
}

async fn create_checkpoint_in_transaction(
    tx: &mut EngineTransaction<'_>,
) -> Result<CreateCheckpointResult, LixError> {
    let version_row = tx
        .execute(
            "SELECT av.version_id, v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await?;
    let [statement] = version_row.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "active version query",
            1,
            version_row.statements.len(),
        ));
    };
    let [row] = statement.rows.as_slice() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "missing active version row".to_string(),
        });
    };
    let version_id = text_at(row, 0, "version_id")?;
    let local_commit_id = text_at(row, 1, "commit_id")?;
    let global_commit_id = load_global_pointer_commit_id(tx).await?;

    let commit = load_commit(tx, &local_commit_id)
        .await?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("commit '{local_commit_id}' is missing"),
        })?;
    if commit.change_set_id.trim().is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("commit '{local_commit_id}' has empty change_set_id"),
        });
    }

    ensure_checkpoint_label_on_commit(tx, &local_commit_id).await?;
    if global_commit_id != local_commit_id {
        ensure_checkpoint_label_on_commit(tx, &global_commit_id).await?;
    }
    tx.execute_internal(
        "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
         VALUES ($1, $2) \
         ON CONFLICT (version_id) DO UPDATE \
         SET checkpoint_commit_id = excluded.checkpoint_commit_id",
        &[
            Value::Text(version_id),
            Value::Text(local_commit_id.clone()),
        ],
    )
    .await?;
    tx.execute_internal(
        "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
         VALUES ($1, $2) \
         ON CONFLICT (version_id) DO UPDATE \
         SET checkpoint_commit_id = excluded.checkpoint_commit_id",
        &[
            Value::Text(crate::version::GLOBAL_VERSION_ID.to_string()),
            Value::Text(global_commit_id.clone()),
        ],
    )
    .await?;

    Ok(CreateCheckpointResult {
        id: local_commit_id,
        change_set_id: commit.change_set_id,
    })
}

#[derive(Debug, Clone)]
struct CommitRow {
    change_set_id: String,
}

async fn load_commit(
    tx: &mut EngineTransaction<'_>,
    commit_id: &str,
) -> Result<Option<CommitRow>, LixError> {
    let result = tx
        .execute_internal(
            "SELECT lix_json_extract(snapshot_content, 'change_set_id') \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_commit' \
               AND entity_id = $1 \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC, created_at DESC, change_id DESC \
             LIMIT 1",
            &[Value::Text(commit_id.to_string())],
        )
        .await?;
    let [statement] = result.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "load commit query",
            1,
            result.statements.len(),
        ));
    };
    let Some(row) = statement.rows.first() else {
        return Ok(None);
    };
    Ok(Some(CommitRow {
        change_set_id: text_at(row, 0, "change_set_id")?,
    }))
}

async fn load_global_pointer_commit_id(tx: &mut EngineTransaction<'_>) -> Result<String, LixError> {
    let result = tx
        .execute_internal(
            "SELECT lix_json_extract(snapshot_content, 'commit_id') \
             FROM lix_internal_state_materialized_v1_lix_global_pointer \
             WHERE schema_key = 'lix_global_pointer' \
               AND entity_id = 'global' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND global = true \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC, created_at DESC, change_id DESC \
             LIMIT 1",
            &[],
        )
        .await?;
    let [statement] = result.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "global pointer commit query",
            1,
            result.statements.len(),
        ));
    };
    let Some(row) = statement.rows.first() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "global pointer row is missing".to_string(),
        });
    };
    text_at(row, 0, "lix_global_pointer.commit_id")
}

async fn ensure_checkpoint_label_on_commit(
    tx: &mut EngineTransaction<'_>,
    commit_id: &str,
) -> Result<(), LixError> {
    let checkpoint_label_id = load_checkpoint_label_id(tx).await?;
    let entity_label_id = format!("{commit_id}~lix_commit~lix~{checkpoint_label_id}");
    let exists = tx
        .execute(
            "SELECT 1 \
             FROM lix_entity_label \
             WHERE entity_id = $1 \
               AND schema_key = 'lix_commit' \
               AND file_id = 'lix' \
               AND label_id = $2 \
             LIMIT 1",
            &[
                Value::Text(commit_id.to_string()),
                Value::Text(checkpoint_label_id.clone()),
            ],
        )
        .await?;
    let [statement] = exists.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "entity label existence query",
            1,
            exists.statements.len(),
        ));
    };
    if !statement.rows.is_empty() {
        return Ok(());
    }

    tx.execute_internal(
        "DELETE FROM lix_internal_state_vtable \
         WHERE entity_id = $1 \
           AND schema_key = 'lix_entity_label' \
           AND file_id = 'lix' \
           AND version_id = 'global'",
        &[Value::Text(entity_label_id.clone())],
    )
    .await?;
    tx.execute_internal(
        "INSERT INTO lix_internal_state_vtable (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
         ) VALUES ($1, 'lix_entity_label', 'lix', 'global', 'lix', $2, '1', true)",
        &[
            Value::Text(entity_label_id),
            Value::Text(
                serde_json::json!({
                    "entity_id": commit_id,
                    "schema_key": "lix_commit",
                    "file_id": "lix",
                    "label_id": checkpoint_label_id,
                })
                .to_string(),
            ),
        ],
    )
    .await?;
    Ok(())
}

async fn load_checkpoint_label_id(tx: &mut EngineTransaction<'_>) -> Result<String, LixError> {
    let result = tx
        .execute_internal(
            "SELECT snapshot_content \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_label' \
               AND file_id = 'lix' \
               AND version_id = 'global' \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await?;
    let [statement] = result.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "checkpoint label lookup query",
            1,
            result.statements.len(),
        ));
    };
    for row in &statement.rows {
        let snapshot_content = text_at(row, 0, "snapshot_content")?;
        let parsed: serde_json::Value =
            serde_json::from_str(&snapshot_content).map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("checkpoint label snapshot invalid JSON: {error}"),
            })?;
        if parsed.get("name").and_then(serde_json::Value::as_str) != Some("checkpoint") {
            continue;
        }
        if let Some(label_id) = parsed
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.is_empty())
        {
            return Ok(label_id.to_string());
        }
    }
    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "checkpoint label row is missing in the global lane".to_string(),
    })
}

fn text_at(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Text(_)) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{field} is empty"),
        }),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text-like value for {field}, got {other:?}"),
        }),
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("missing {field}"),
        }),
    }
}
