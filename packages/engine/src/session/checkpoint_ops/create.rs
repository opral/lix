use crate::canonical::load_commit as load_canonical_commit;
use crate::canonical::{
    checkpoint_commit_label_entity_id, checkpoint_commit_label_snapshot,
    CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY,
};
use crate::functions::FunctionBindings;
use crate::version::GLOBAL_VERSION_ID;
use crate::{ExecuteOptions, LixError, Session, SessionTransaction, Value};

use super::super::version_ops::context::require_target_version_context_in_transaction;

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
    let active_context = require_target_version_context_in_transaction(
        tx,
        None,
        "active_version_id",
        "active version",
    )
    .await?;
    let global_context = require_target_version_context_in_transaction(
        tx,
        Some(GLOBAL_VERSION_ID),
        "version_id",
        "global version",
    )
    .await?;
    let version_id = active_context.version_id().to_string();
    let local_commit_id = active_context.head_commit_id().to_string();
    let global_commit_id = global_context.head_commit_id().to_string();

    let commit = load_tracked_commit_header(tx, &local_commit_id)
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
    // Keep the derived checkpoint-history cache warm for the active version.
    crate::session::checkpoint_ops::cache::upsert_last_checkpoint_for_version_in_transaction(
        tx.backend_transaction_mut()?,
        &version_id,
        &local_commit_id,
    )
    .await?;
    // The global lane mirrors the same derived cache contract and remains
    // rebuildable from canonical heads plus canonical checkpoint labels.
    crate::session::checkpoint_ops::cache::upsert_last_checkpoint_for_version_in_transaction(
        tx.backend_transaction_mut()?,
        GLOBAL_VERSION_ID,
        &global_commit_id,
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

async fn load_tracked_commit_header(
    tx: &mut SessionTransaction<'_>,
    commit_id: &str,
) -> Result<Option<CommitRow>, LixError> {
    // Checkpoint creation only needs the tracked commit header fact; it does
    // not traverse commit-member change rows here.
    let mut executor = tx.backend_transaction_mut()?;
    let Some(commit) = load_canonical_commit(&mut executor, commit_id).await? else {
        return Ok(None);
    };
    Ok(Some(CommitRow {
        change_set_id: commit.change_set_id.unwrap_or_default(),
    }))
}

async fn ensure_checkpoint_label_on_commit(
    tx: &mut SessionTransaction<'_>,
    commit_id: &str,
) -> Result<(), LixError> {
    let state_entity_id = checkpoint_commit_label_entity_id(commit_id);
    let exists = tx
        .backend_transaction_mut()?
        .execute(
            "SELECT 1 \
             FROM lix_internal_change \
             WHERE entity_id = $1 \
               AND schema_key = $2 \
               AND file_id IS NULL \
               AND plugin_key IS NULL \
             LIMIT 1",
            &[
                Value::Text(state_entity_id.clone()),
                Value::Text(CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY.to_string()),
            ],
        )
        .await?;
    if !exists.rows.is_empty() {
        return Ok(());
    }

    let snapshot_content = checkpoint_commit_label_snapshot(commit_id);
    let change_id = generate_runtime_uuid(tx).await?;
    let timestamp = generate_runtime_timestamp(tx).await?;
    crate::live_state::write_live_rows(
        tx.backend_transaction_mut()?,
        &[crate::live_state::LiveRow {
            entity_id: state_entity_id.clone(),
            file_id: None,
            schema_key: CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY.to_string(),
            schema_version: "1".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            plugin_key: None,
            metadata: None,
            change_id: Some(change_id.clone()),
            writer_key: None,
            global: true,
            untracked: false,
            created_at: Some(timestamp.clone()),
            updated_at: Some(timestamp.clone()),
            snapshot_content: Some(snapshot_content.clone()),
        }],
    )
    .await?;
    insert_canonical_checkpoint_label_change(
        tx,
        &state_entity_id,
        &snapshot_content,
        &change_id,
        &timestamp,
    )
    .await?;
    Ok(())
}

async fn insert_canonical_checkpoint_label_change(
    tx: &mut SessionTransaction<'_>,
    entity_id: &str,
    snapshot_content: &str,
    change_id: &str,
    created_at: &str,
) -> Result<(), LixError> {
    let snapshot_id = format!("{change_id}~snapshot");
    tx.backend_transaction_mut()?
        .execute(
            "INSERT INTO lix_internal_snapshot (id, content) \
             SELECT $1, $2 \
             WHERE NOT EXISTS (SELECT 1 FROM lix_internal_snapshot WHERE id = $1)",
            &[
                Value::Text(snapshot_id.clone()),
                Value::Text(snapshot_content.to_string()),
            ],
        )
        .await?;
    tx.backend_transaction_mut()?
        .execute(
            &format!(
                "INSERT INTO lix_internal_change (\
             id, entity_id, schema_key, schema_version, file_id, plugin_key, snapshot_id, metadata, created_at\
             ) \
             SELECT $1, $2, '{schema_key}', '1', NULL, NULL, $3, NULL, $4 \
             WHERE NOT EXISTS (SELECT 1 FROM lix_internal_change WHERE id = $1)",
                schema_key = CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY,
            ),
            &[
                Value::Text(change_id.to_string()),
                Value::Text(entity_id.to_string()),
                Value::Text(snapshot_id),
                Value::Text(created_at.to_string()),
            ],
        )
        .await?;
    Ok(())
}

async fn checkpoint_function_bindings(
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

async fn generate_runtime_uuid(tx: &mut SessionTransaction<'_>) -> Result<String, LixError> {
    Ok(checkpoint_function_bindings(tx)
        .await?
        .provider()
        .call_uuid_v7())
}

async fn generate_runtime_timestamp(tx: &mut SessionTransaction<'_>) -> Result<String, LixError> {
    Ok(checkpoint_function_bindings(tx)
        .await?
        .provider()
        .call_timestamp())
}
