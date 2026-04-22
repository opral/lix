use crate::canonical::load_commit as load_canonical_commit;
use crate::canonical::{checkpoint_commit_label_entity_id, CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY};
use crate::functions::FunctionBindings;
use crate::transaction::{
    append_checkpoint_commit_label_fact_in_transaction, CheckpointCommitLabelWrite,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{ExecuteOptions, LixError, Session, SessionTransaction};

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
            hint: None,
        })?;
    if commit.change_set_id.trim().is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("commit '{local_commit_id}' has empty change_set_id"),
            hint: None,
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
    if crate::session::checkpoint_ops::storage::checkpoint_commit_label_exists_in_transaction(
        tx.backend_transaction_mut()?,
        &state_entity_id,
        CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY,
    )
    .await?
    {
        return Ok(());
    }

    let function_bindings = checkpoint_function_bindings(tx).await?;
    let change_id = function_bindings.provider().call_uuid_v7();
    let timestamp = function_bindings.provider().call_timestamp();
    let mut functions = function_bindings.provider().clone();
    append_checkpoint_commit_label_fact_in_transaction(
        tx.backend_transaction_mut()?,
        &mut functions,
        &CheckpointCommitLabelWrite {
            commit_id: commit_id.to_string(),
            change_id,
            created_at: timestamp,
        },
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
