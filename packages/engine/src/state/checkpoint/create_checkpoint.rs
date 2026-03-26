use super::{checkpoint_commit_label_entity_id, checkpoint_commit_label_snapshot};
use crate::{errors, ExecuteOptions, LixError, Session, SessionTransaction, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateCheckpointResult {
    pub id: String,
    pub change_set_id: String,
}

pub(crate) async fn create_checkpoint_in_session(
    session: &Session,
) -> Result<CreateCheckpointResult, LixError> {
    session
        .transaction(ExecuteOptions::default(), |tx| {
            Box::pin(async move { create_checkpoint_in_transaction(tx).await })
        })
        .await
}

async fn create_checkpoint_in_transaction(
    tx: &mut SessionTransaction<'_>,
) -> Result<CreateCheckpointResult, LixError> {
    let version_id = tx.context.active_version_id.clone();
    let local_commit_id = load_version_commit_id(tx, &version_id).await?;
    let global_commit_id = load_global_version_commit_id(tx).await?;

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
    tx: &mut SessionTransaction<'_>,
    commit_id: &str,
) -> Result<Option<CommitRow>, LixError> {
    let result = tx
        .execute(
            "SELECT change_set_id \
             FROM lix_commit \
             WHERE id = $1 \
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

async fn load_global_version_commit_id(
    tx: &mut SessionTransaction<'_>,
) -> Result<String, LixError> {
    let result = tx
        .execute(
            "SELECT commit_id \
             FROM lix_version \
             WHERE id = $1 \
             LIMIT 1",
            &[Value::Text(crate::version::GLOBAL_VERSION_ID.to_string())],
        )
        .await?;
    let [statement] = result.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "hidden global version commit query",
            1,
            result.statements.len(),
        ));
    };
    let Some(row) = statement.rows.first() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "hidden global version ref row is missing".to_string(),
        });
    };
    text_at(row, 0, "lix_version.commit_id")
}

async fn load_version_commit_id(
    tx: &mut SessionTransaction<'_>,
    version_id: &str,
) -> Result<String, LixError> {
    let result = tx
        .execute(
            "SELECT commit_id \
             FROM lix_version \
             WHERE id = $1 \
             LIMIT 1",
            &[Value::Text(version_id.to_string())],
        )
        .await?;
    let [statement] = result.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "active version query",
            1,
            result.statements.len(),
        ));
    };
    let Some(row) = statement.rows.first() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("version '{version_id}' is missing"),
        });
    };
    text_at(row, 0, "lix_version.commit_id")
}

async fn ensure_checkpoint_label_on_commit(
    tx: &mut SessionTransaction<'_>,
    commit_id: &str,
) -> Result<(), LixError> {
    let state_entity_id = checkpoint_commit_label_entity_id(commit_id);
    let exists = tx
        .execute_internal(
            "SELECT 1 \
             FROM lix_state_by_version \
             WHERE entity_id = $1 \
               AND schema_key = 'lix_entity_label' \
               AND file_id = 'lix' \
               AND version_id = $2 \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            &[
                Value::Text(state_entity_id.clone()),
                Value::Text(crate::version::GLOBAL_VERSION_ID.to_string()),
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
        "INSERT INTO lix_state_by_version (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
         ) VALUES ($1, 'lix_entity_label', 'lix', $2, 'lix', $3, '1', true)",
        &[
            Value::Text(state_entity_id),
            Value::Text(crate::version::GLOBAL_VERSION_ID.to_string()),
            Value::Text(checkpoint_commit_label_snapshot(commit_id)),
        ],
    )
    .await?;
    Ok(())
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
